# Datatype Fidelity

How each Spark type is stored in Elasticsearch on write, and how it is read back. The two
directions share one contract: the read is the exact inverse of the write, except for the one-way
deltas listed below.

- [Write: Spark to Elasticsearch](#write-spark-to-elasticsearch)
- [Arrow-hostile types: `variant` and `interval`](#arrow-hostile-types-variant-and-interval-handled-automatically)
- [Read: Elasticsearch to Spark](#read-elasticsearch-to-spark)

## Write: Spark to Elasticsearch

This table is the **write** transform (Spark → ES `_source`); the
[read table](#read-elasticsearch-to-spark) below is its inverse (ES `_source` → Spark) and
lists which of these transforms are one-way. Both directions share this one contract.

Every valid Spark column can be exported with **no caller pre-processing**: hand `bulk_write` any
DataFrame. Values are transformed on the way to Elasticsearch as follows. These transforms are
**by design**: they are the deltas to expect between the Spark row and the ES `_source`, not bugs.

| Spark type | ES `_source` value | Example (input → stored) |
|---|---|---|
| `string`, `boolean` | unchanged | `"hi"` → `"hi"`; `true` → `true` |
| `byte`/`short`/`int`/`long` | unchanged (all integer widths become one JSON number; width not preserved) | `5` → `5` |
| `double` | unchanged; non-finite values (`Infinity`/`-Infinity`/`NaN`) become `null` (no JSON representation) | `1.5` → `1.5`; `Infinity` → `null` |
| `float` (32-bit) | its **short decimal representation** (the shortest string that round-trips to the same float32), so it reads back into a `FLOAT` unchanged | `0.1` (float) → `0.1` |
| `decimal(p,s)` | rendered at **full precision** (a JSON number), keeping the declared scale; read back exactly only when written without a decimal point (scale 0). A `decimal(p,s)` with `s>0` writes trailing fractional zeros and parses to a float on read, losing low digits past ~15-17 sig figs even for an integral value (see note) | `decimal(38,0)` `123456789012345678` → `123456789012345678` (exact); the same value as `decimal(38,2)` → lossy on read |
| `date` / `timestamp` | **epoch milliseconds** (integer), floored to the millisecond (sub-ms precision dropped). The `timestamp` epoch is the true UTC instant, independent of `spark.sql.session.timeZone` (see below) | `2021-01-01T00:00:00Z` → `1609459200000` |
| `timestamp_ntz` | **epoch milliseconds** of the wall-clock read as UTC (see below) | `2021-06-01 12:00:00` → `1622548800000` |
| `binary` | **base64 string** | `b"\x01\x02"` → `"AQI="` |
| `struct` / `map` | nested object (recursed). Non-string `map` keys are rendered to strings (JSON keys must be strings) by `to_json`, which uses the key's **own** string form, **not** the value transform: a `timestamp`/`date`/`timestamp_ntz` **key** becomes an ISO string (a value of the same type becomes epoch-millis). Map **values** follow the value transforms as usual (see note) | `{a: 1}` → `{"a": 1}`; `map<int,_>` `{1: "x"}` → `{"1": "x"}` |
| `array` | array (recursed) | `[1, 2]` → `[1, 2]` |
| `null` (any type) | present as JSON `null` (the field is kept, its value is `null`) | `None` → `null` |
| `variant` | **string containing serialized JSON** (see below) | `{"k": 1}` → `"{\"k\":1}"` |
| `interval` | **string** (see below) | `INTERVAL '1 02:03:04' DAY TO SECOND` → `"INTERVAL '1 02:03:04' DAY TO SECOND"` |

**`float` (32-bit) round-trips faithfully:** `to_json` renders a `FLOAT` as its **short decimal
representation** (the shortest string that parses back to the same 32-bit float), so `0.1f` stores
as `0.1`, not the widened double `0.10000000149011612`. Read back into a `FLOAT` column it returns to
the same float32, so there is no float32 widening delta to expect. This exact round-trip depends on
Spark's `to_json` rendering a `FLOAT` at float precision rather than widening it to a double; it is a
Spark behavior the integration tier pins (`test_datatype_coverage::test_float32_short_repr_roundtrips`),
not a guarantee the connector code makes on its own, so a future change to that rendering would
resurface the delta (and the tier would catch it).

**`decimal` beyond double precision (scale matters):** `to_json` writes a decimal at full precision
into `_source`, keeping the declared scale. At **scale 0** the number is a bare integer literal, so it
round-trips exactly however many digits it has (a JSON integer parses to an arbitrary-precision Python
int). At **scale > 0** the number carries a decimal point (a `decimal(38,2)` value of
`123456789012345678` is written `123456789012345678.00`), and a JSON number with a decimal point parses
to a **double** on read, so it loses low digits past ~15-17 significant figures **even when the value
is integral**, not only for fractional values. If you need exact high-precision decimals, cast the
column to `string` in Spark before writing (declare `StringType` on read); that is the documented
workaround and round-trips exactly at any precision or scale.

**Non-string `map` keys use their own string form:** `to_json` stringifies a non-string map key to its
own representation, which for a temporal key (`timestamp`/`date`/`timestamp_ntz`) is an **ISO string**,
not the epoch-millis a value of that type gets. `int`/`bigint`/`string` keys render as you'd expect.
A non-finite float used as a map **key** is stringified to `"NaN"`/`"Infinity"`/`"-Infinity"` (not
turned to `null` like a non-finite *value*), since the non-finite rewrite only touches values.
A map **keyed by** a temporal/decimal/binary/float type is an unusual shape; **use `string` or integer
map keys** if you rely on the key's exact form.

**Timestamp precision:** epoch-millis is floored to the containing millisecond, consistently for
pre- and post-epoch instants (matches Spark/Java `unix_millis`). Elasticsearch `date` is
millisecond-resolution by default; sub-millisecond (microsecond) precision from a Spark `timestamp`
is not preserved. Map the field as `date_nanos` and send nanos yourself if you need finer than ms.

**`timestamp` vs `timestamp_ntz`:** a regular Spark `timestamp` is an instant and converts to its
true epoch-millis. A `timestamp_ntz` (no time zone) carries a wall-clock value with no zone, so the
connector interprets it as **UTC**, deterministically, since executors can be in different zones.
The stored epoch-millis is that wall-clock time read as UTC, not as the cluster's local zone. If
your NTZ values are really in some other zone, convert them to a zoned `timestamp` in Spark before
export so the epoch is correct.

**Timezone independence (`timestamp`):** the epoch a `timestamp` stores is its **true UTC instant,
regardless of `spark.sql.session.timeZone`**: set the session to `America/New_York`, `Asia/Kolkata`,
or anything else and the same instant produces the same epoch. The connector guarantees this by
converting every `timestamp` column (at any nesting depth: inside `struct`/`array`/`map` too) to its
epoch-millis in Spark via `unix_millis` **before** the export, rather than letting Spark's Arrow
conversion render it to the session-local wall-clock. This matters because a naive
(session-local) render would otherwise be stored as if it were UTC, silently shifting the stored
instant by the session's offset. No caller action is needed, and the session is never mutated. Only
`timestamp` is converted this way; `timestamp_ntz` (defined as UTC above) and `date` (no
time-of-day) are already session-independent and are left untouched.

## Arrow-hostile types: `variant` and `interval` (handled automatically)

`VARIANT` and `INTERVAL` have no Apache Arrow representation, so they cannot cross into
`mapInPandas` directly. `bulk_write` handles this for you: any column whose type **contains** one
of these at **any nesting depth** is serialized to a **string** before export, so no caller action
is required; hand `bulk_write` the raw DataFrame. The string form depends on the type:

- **`variant`** (at any depth, e.g. `variant`, `struct<...,v:variant>`, `array<struct<...,v:variant>>`)
  → a **JSON string**. Example: a `variant` holding `{"k": 1, "nested": [2, 3]}` is stored as the
  string `"{\"k\":1,\"nested\":[2,3]}"`.
- **`interval`** (day-time or year-month) → its **Spark string form**. Example:
  `INTERVAL '1 02:03:04' DAY TO SECOND` is stored as the string `"INTERVAL '1 02:03:04' DAY TO SECOND"`.

Round-trip consequence to expect: such a column lands in ES as a **string**, not a queryable nested
object, so map it as `keyword`/`text`, not `object`.

When adding fields, add a matching entry to the ES index mapping or ES will dynamic-map (and guess)
the type.

## Read: Elasticsearch to Spark

Reads honor the **same contract as writes**: `read_index(cfg, df.schema)` after `bulk_write(df,
cfg)` reproduces the original DataFrame, **except** the deltas the
[write table](#write-spark-to-elasticsearch) above documents as one-way:
sub-millisecond timestamp truncation, and a `decimal` whose stored JSON number is parsed to a double
on read (exact only at scale 0, where `to_json` writes a bare integer literal; a `decimal(p,s)` with
`s>0` writes a decimal point and loses low digits past ~15-17 sig figs even for an integral value).
Each read coercion is the exact inverse of the write transform:

| Declared Spark type | Stored in ES | Read back as |
|---|---|---|
| `timestamp` / `date` | epoch-millis integer | `datetime` / `date` (UTC) |
| `timestamp_ntz` | epoch-millis integer | naive `datetime` (no tzinfo, the UTC wall-clock) |
| `binary` | base64 string | `bytes` |
| `decimal(p,s)` | full-precision JSON number (the declared scale is kept, so a scale>0 value carries a decimal point) | `Decimal` (exact only when written without a decimal point, i.e. scale 0; a `decimal(p,s)` with `s>0` parses to a float on read and loses low digits past ~15-17 sig figs, integral value or not) |
| `variant` / `interval` | JSON / interval string | string (re-parse with `parse_json` yourself) |
| scalars / `struct` / `array` / `map` | same shape | same, recursively |

What a read rejects instead of coercing (a declared type that does not fit the stored value) is in
[Declaring the schema](reading.md#declaring-the-schema).
