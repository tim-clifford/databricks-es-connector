# Quick Start

The shortest path to a first write and read: one DataFrame into an existing index, then the same
index back into a DataFrame. It shows the minimum settings, not every option; follow the links for
the rest.

## Before you start

- **The connector is installed** in your workspace and the `es` secret scope holds your API key
  ([Installation and Setup](setup.md)).
- **The target index exists** in Elasticsearch, ideally with an explicit mapping. By default the
  connector refuses to write to a missing index, so a typo can't create a new one
  (`require_existing_index`, see [Configuration Reference](configuration.md#write-behavior-eswriteconfig)).

## Write a DataFrame

```python
from databricks_es_connector import EsWriteConfig, bulk_write

cfg = EsWriteConfig(
    hosts="https://es-host:9200",
    api_key=dbutils.secrets.get("es", "api_key"),
    index="my-index",
    id_field="doc_id",       # deterministic _id => idempotent
)
result = bulk_write(df, cfg, raise_on_error=True)   # raises EsWriteError if any doc was rejected or any row lost
print(result)   # -> {"written": N, "errors": 0, "unaccounted": 0, ...}
```

`id_field` makes a re-run an upsert instead of a duplicate. Check the result as described in
[Write Results and Diagnostics](write-results.md). For a streaming source, use `make_foreach_batch`
instead ([Writing to Elasticsearch](writing.md#streaming-make_foreach_batch)).

## Read it back

```python
from databricks_es_connector import EsReadConfig, read_index

read_cfg = EsReadConfig(
    hosts="https://es-host:9200",
    api_key=dbutils.secrets.get("es", "api_key"),
    index="my-index",
    id_field="doc_id",
)
back = read_index(spark, read_cfg, df.schema)   # you declare the schema; the reader does not infer it
```

Passing the original `df.schema` reproduces the DataFrame, apart from the two one-way deltas in
[Datatype Fidelity](datatypes.md#read-elasticsearch-to-spark). A round-trip like this can't verify
your index mapping, though; see [Mapping gotchas](reading.md#mapping-gotchas).

## Next steps

- Every config field: [Configuration Reference](configuration.md)
- Deletes, streaming, and failure handling: [Writing to Elasticsearch](writing.md)
- Retries and backoff under load: [Retries](retries.md)
