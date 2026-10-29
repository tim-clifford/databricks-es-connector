# Writing to Elasticsearch

Write a Spark DataFrame to an index with `bulk_write` (batch) or `make_foreach_batch` (streaming),
configured with an [`EsWriteConfig`](configuration.md#write-behavior-eswriteconfig). This guide covers
both entry points, what happens when a write fails, and deletes. What a write returns is in
[Write Results and Diagnostics](write-results.md); every field is in the
[Configuration Reference](configuration.md).

- [Batch (`bulk_write`)](#batch-bulk_write)
- [Streaming (`make_foreach_batch`)](#streaming-make_foreach_batch)
- [Deletes](#deletes)

## Batch (`bulk_write`)

> **First time in a workspace?** Install the library and create the ES secret scope before
> running any of the snippets below, see [Installation and Setup](setup.md).
> The examples assume `databricks_es_connector` is importable and secrets exist.

```python
from databricks_es_connector import EsWriteConfig, bulk_write

cfg = EsWriteConfig(
    hosts="https://es-host:9200",
    api_key=dbutils.secrets.get("es", "api_key"),
    index="my-index",
    id_field="doc_id",       # deterministic _id => idempotent
    http_compress=True,      # gzip the request body
    drop_fields=("raw_data", "unmapped"),  # prune before indexing
    verify_certs=True,
)
result = bulk_write(gold_df, cfg)   # -> {"written": N, "errors": M, "unaccounted": U, ...}

# Or let the library enforce it: raises EsWriteError if any doc was rejected or any row lost.
result = bulk_write(gold_df, cfg, raise_on_error=True)
```

`bulk_write` returns the counts and leaves the verdict to you (`raise_on_error=False` by default);
see [The write result](write-results.md) for what to check. The **streaming**
entry point defaults the other way and raises, because there a swallowed error silently advances the
checkpoint past the lost rows.

## Streaming (`make_foreach_batch`)

The library exposes `make_foreach_batch(cfg, on_batch=None, on_error="raise")`, a `foreachBatch`
function that bulk-writes each micro-batch:

```python
from databricks_es_connector import EsWriteConfig, make_foreach_batch

cfg = EsWriteConfig(hosts=..., api_key=..., index=..., id_field="doc_id")
(spark.readStream.table("catalog.schema.source_table")
   .writeStream
   .foreachBatch(make_foreach_batch(cfg, on_batch=lambda bid, r: print(bid, r)))
   .option("checkpointLocation", "/Volumes/<cat>/<schema>/<vol>/es_ckpt")  # UC Volume
   .trigger(availableNow=True)   # only availableNow/Once on serverless
   .start())
```

Notes for serverless:

- **Only `availableNow`/`Once` triggers work** on serverless. Each run drains the source
  commits appended since the last run; the checkpoint tracks the offset (incremental tailing).
- **The checkpoint must live on a UC Volume** (`dbfs:/tmp` paths fail with
  `INSUFFICIENT_PERMISSIONS`). One checkpoint dir = one logical stream.
- **Run streaming as a Databricks job, not interactively.** Driving a `foreachBatch` stream
  cell-by-cell on serverless / Spark Connect is unreliable (`query.start()` can intermittently
  hang). Wrap it in a job that calls `spark.streams.awaitAnyTermination()`. See
  [Production Readiness](production-readiness.md) for the details and the at-least-once / freshness caveats.
- **`on_batch` result shape.** The dict passed to your `on_batch` callback always carries the same
  keys `bulk_write` returns (`written`/`deleted`/`errors`/`ignored`/`coerced_nonfinite`/
  `total_input`/`unaccounted`/`error_samples`). An **empty** micro-batch (no rows) skips the write
  and additionally sets `empty: True`; that flag is present only on skipped batches, so read it with
  `result.get("empty")`, not `result["empty"]`.

### A failed micro-batch fails the stream (`on_error`, default `"raise"`)

Structured Streaming commits a micro-batch's checkpoint offset when `foreachBatch` **returns
normally**, with no visibility into what happened to the documents inside. So a helper that returned
after a failed write would advance the offset past the rejected rows and they would never be
retried: silent, permanent loss behind a green job.

`make_foreach_batch` therefore **raises `EsWriteError` by default**, failing the batch so Spark
retries it and the checkpoint holds. With `id_field` set, that retry is an idempotent upsert rather
than a duplicate.

```python
fb = make_foreach_batch(cfg)                      # raise, checkpoint holds
fb = make_foreach_batch(cfg, on_error="log")      # warn, checkpoint ADVANCES (rows not retried)
fb = make_foreach_batch(cfg, on_error="ignore")   # same, silently
```

A write counts as failed when `errors > 0` (ES rejected documents) or `unaccounted > 0` (rows lost
below the per-document level). Delete-404 no-ops (`ignored`) are not failures, so a CDF replay that
deletes already-absent docs does not trip it. `on_batch` runs *before* the policy, so a metrics or
dead-letter hook still sees the failing batch.

> **Triage note.** `ConnectionError`, `ConnectionTimeout` and `SerializationError` are not `ApiError`
> subclasses, so the connector's `raise_on_exception=False` does not suppress them: a network failure
> propagates, fails the Spark task, and is retried by Spark. Only per-document rejections are returned
> as counts, which is what the `on_error` policy acts on.

## Deletes

By default the connector only inserts and updates (replace-by-`_id`). To also propagate
**deletes**, set `has_deletes=True` and name a `delete_flag_column`: rows whose flag is truthy
are sent to ES as a delete-by-`_id`; all other rows index as usual.

```python
cfg = EsWriteConfig(
    hosts=..., api_key=..., index="my-index",
    id_field="doc_id",              # required for deletes: you delete by _id
    has_deletes=True,
    delete_flag_column="_is_delete",  # truthy row => delete that _id from ES
)
result = bulk_write(prepared_df, cfg)   # -> {"written": N, "deleted": D, "errors": M, "total_input": T, "error_samples": [...]}
```

The connector is schema-agnostic about *how* you decide the latest state of a row: it deletes
exactly the rows you flag and indexes the rest. A common source is a Delta **Change Data Feed**:
you dedup to one record per id in Spark and set the flag from `_change_type == 'delete'`. That
ordering/dedup work stays in your pipeline, not in the connector, because it needs your business
sequencing column (e.g. `event_ts`) and the sort should run distributed in Spark rather than in
executor memory. A typical `foreachBatch` shape:

```python
from pyspark.sql import functions as F, Window

def upsert_latest(batch_df, batch_id):
    # keep the latest record per id: business time first, commit version as tie-breaker
    w = Window.partitionBy("id").orderBy(
        F.col("event_ts").desc_nulls_last(),
        F.col("_commit_version").desc(),
    )
    latest = (
        batch_df
        .withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
        .withColumn("_is_delete", F.col("_change_type") == "delete")
    )
    make_foreach_batch(cfg, on_batch=on_batch)(latest, batch_id)  # cfg has has_deletes=True
```

**Ordering caveat:** dedup like the above is only authoritative *within* one micro-batch. Across
batches, Elasticsearch applies writes in arrival/commit order, so a late-arriving row whose
`event_ts` is older than a doc already in ES, but committed in a later batch, can overwrite the
newer doc. If your source can deliver out-of-order events across batches, make `event_ts` globally
authoritative with ES external versioning (`version` = `event_ts` epoch-millis,
`version_type=external_gte`) so ES itself rejects stale writes regardless of batch order.
