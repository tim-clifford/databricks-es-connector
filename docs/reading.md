# Reading from Elasticsearch

Read an index back into a Spark DataFrame with `read_index`, configured with an
[`EsReadConfig`](configuration.md#read-behavior-esreadconfig). This guide covers the read itself, the
schema you must declare, and the Elasticsearch mapping behaviors that surprise a reader. How each type
is stored and read back is in [Datatype Fidelity](datatypes.md).

- [`read_index`](#read_index)
- [Declaring the schema](#declaring-the-schema)
- [Mapping gotchas](#mapping-gotchas)

## `read_index`

Read an Elasticsearch index back into a Spark DataFrame. **You must declare the target Spark
schema**: the reader does not infer it (see [Declaring the schema](#declaring-the-schema) for why).

```python
from databricks_es_connector import EsReadConfig, read_index
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

schema = StructType([
    StructField("doc_id", StringType()),
    StructField("event_ts", TimestampType()),   # stored as epoch-millis, read back as timestamp
    StructField("n", LongType()),
])

cfg = EsReadConfig(
    hosts="https://es-host:9200",
    api_key=dbutils.secrets.get("es", "api_key"),
    index="my-index",
    id_field="doc_id",               # filled from the ES _id if absent from _source
    query={"term": {"region.keyword": "us"}},  # optional raw ES query DSL; omit for match_all (.keyword: see below)
    pit_keep_alive="5m",             # sliding window: covers the gap between PIT touches, not the whole job (see note)
)
df = read_index(spark, cfg, schema)  # distributed: one Spark task per shard/slice
```

> **`term` gotcha:** dynamically-mapped string fields are `text` (analyzed), so a `term` query on
> the bare field usually returns nothing. Query the exact `.keyword` sub-field
> (`{"term": {"region.keyword": "us"}}`) or use `match`. Standard Elasticsearch behavior; check with
> `GET /<index>/_mapping/field/<field>`.

**`read_index` is distributed** and serverless-safe: it opens one Elasticsearch Point-in-Time for a
consistent snapshot, then fans out `spark.range(num_slices).mapInPandas(...)`, one task per PIT
slice (defaulting to the index's shard count), each task paging its slice with `search_after`. The
data stays across executors (never collected to the driver), so it scales for full-index export.

- **PIT lifetime is a sliding window, not a total budget.** The returned DataFrame is **lazy**:
  Spark reads each slice when an action runs, not when `read_index` returns. Every page request
  re-sends `pit_keep_alive` and resets the Point-in-Time's expiry, so `pit_keep_alive` only has to
  cover the longest gap *between* consecutive reads of the PIT, not the whole job's wall-clock: the
  open-PIT-to-first-page gap (the read is lazy, so Spark may schedule the tasks late, plus
  serverless executor cold-start) and any gap between pages if a slow consumer applies backpressure.
  The `5m` default covers a normal scheduling gap; raise it only if one of those gaps runs longer.
  (The PIT can't be closed on the driver without killing the still-lazy read, so it's left to expire
  on its own once reads stop touching it.)
- **For small/bounded reads** (lookups, reference data), pass `num_slices=1` for a single unsliced
  reader; the returned DataFrame is still lazy and distributed, just one task instead of a fan-out.

## Declaring the schema

How each declared type is coerced on read is in the [read table](datatypes.md#read-elasticsearch-to-spark).

**Why the schema is required:** an epoch-millis integer in `_source` could be a `timestamp`, a
`date`, or a genuine `long`; a base64 string could be `binary` or a real `keyword`. The stored value
alone is ambiguous, so the reader must be told the intended type. There is no mapping inference:
declare the schema explicitly. (ES also has no array type: a field declared `array<T>` is read as a
list even if ES returned a scalar.)

**Maps read back as `map<string,V>`:** JSON object keys are always strings, and the write path
stringifies every non-string map key, so map keys can only be read back as strings. A declared
`map<K,V>` whose key type `K` is not `string` therefore cannot round-trip, and `read_index` rejects
it with `ReadSchemaMismatch` naming the fix (the original key type is not recoverable from `_source`);
the map values are unaffected. `read_index` also returns **only the fields in the declared schema**:
any other field present in `_source` is not read back.

**A column name that literally contains a dot** (a column named `a.b`, not a `struct` field)
round-trips on an ordinary index but is hazardous: ES maps it as a nested object (`a` with subfield
`b`), so it collides at write time with any sibling field named `a` of a different type (the document
is rejected, visible in the write result's `errors`), and under a synthetic-`_source` index mode
(logsdb / time series) `_source` is reconstructed from the mapping, so a column declared `a.b` reads
back `null`. Dotted ECS-style fields that arrive as Spark `struct`s (`source.ip` as a struct) render
as nested objects and are safe; the hazard is only a name that literally contains a dot.

**A declared type that doesn't fit the stored value raises `ReadSchemaMismatch`** rather than
coercing, because every coercion here yields plausible-looking wrong data that nothing downstream can
detect:

| Stored in ES | Declared | Result |
|---|---|---|
| `["prod", "urgent"]` | `string` | raises: declare `array<string>` |
| `{"k": 1}` | `string` | raises: declare a matching `struct` |
| `3.7` | `int` | raises: lossy |
| `3.0` | `int` | `3` (exact, allowed) |
| `10000000000` | `int` | raises: exceeds the declared width's range (see below) |
| `"maybe"` | `boolean` | raises: no defined meaning (see below) |
| any | `char` / `varchar` / `void` | raises: unsupported declared type (see below) |

**Booleans stored as strings are parsed, not trusted to `bool()`.** Reading an index the connector
did not write, a field may hold the *string* `"false"`, and `bool("false")` is `True`, so the value
would invert with nothing to notice it. Strings are matched against an explicit allow-list:
`"true"/"t"/"1"/"yes"/"y"` and `"false"/"f"/"0"/"no"/"n"`, case-insensitive and whitespace-trimmed.
Anything else raises. Real JSON booleans and `0`/`1` are unaffected.

**`char`, `varchar` and `void` cannot be declared.** Spark cannot carry `CharType`, `VarcharType`, or
`NullType` through the `mapInPandas` the reader uses, so they raise `ReadSchemaMismatch` naming the
fix (`StringType`, or the column's real type). They fail fast at coercion rather than surfacing later
as Spark's `Invalid return type in mapInPandas`, which names neither the field nor the cause.

**An integer value that exceeds the declared width raises, it is not wrapped.** A stored value that
fits an integer type but overflows the declared width (e.g. `10000000000` read as `int`, or `128`
read as `byte`) raises `ReadSchemaMismatch` naming a wider type. Without this, the returned value
must still be cast to the declared type by `mapInPandas`, and that cast either raises a cause-less
`ArrowInvalid` or, on runtimes where `spark.sql.execution.pandas.convertToArrowArraySafely` defaults
`false` (Spark 3.5 / DBR 15.x and earlier), **silently wraps** it (`10000000000` → `1410065408`). A
pure `read_index(cfg, df.schema)` round-trip never hits this (the declared width equals the width
written), so it only arises reading an index the connector did not write, or one whose values
outgrew an under-sized declared type. Declare `short`/`int`/`long`, or a `decimal`/`string` wide
enough to hold the value.

Watch the multi-value case: **ES has no array type**, so any field can hold multiple values under the
same mapping and `GET /_mapping` will not tell you which do. Declare `array<T>` wherever
multi-values are possible; a single stored scalar still reads back as a one-element list. Only lossy
numeric conversions are rejected, since JSON has one number type and an integral float arriving for an
`int` column is normal.

## Mapping gotchas

> **Dynamic-mapping gotcha:** if you don't pre-create an index mapping, ES infers each field's type
> from the **first** document it sees and keeps it. A later doc whose value doesn't fit is silently
> coerced for indexing (a `float` `1.5` into a field first seen as an integer indexes as `1`; a
> keyword string past `ignore_above`, default 256 chars, isn't indexed). This does **not** affect the
> connector round-trip: reads come from `_source`, which ES stores verbatim, so `read_index` returns
> your original value regardless. It only bites when you **query ES directly** (aggregations, sort,
> `term`), where you see the coerced/truncated *indexed* value, not `_source`. For heterogeneous or
> long-string fields, pre-create an explicit mapping. (Same root cause as the `term`/`.keyword`
> gotcha above.)

> **A `read_index` round-trip cannot verify a mapping.** Follows from the note above: reads come from
> `_source`, which is a verbatim copy of what you sent, so a write-then-read-back comparison compares
> your data against a copy of itself and **cannot fail** on a mapping mismatch. With `amount`
> dynamically mapped `long`, writing `100.75` gives `errors=0` and round-trips as `100.75` while the
> indexed value is `100` and `range: {amount: {gt: 100.5}}` finds nothing. To verify a mapping, read
> the **indexed** value: a search with `"fields": [...]`, an aggregation, or `GET /<index>/_mapping`
> compared against the schema you intended.
