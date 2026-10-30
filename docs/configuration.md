# Configuration Reference

Every config field, its type, default, and what it does. Other docs link here instead of repeating
these definitions.

- [Connection (shared by both configs)](#connection-shared-by-both-configs)
- [Write behavior (`EsWriteConfig`)](#write-behavior-eswriteconfig)
- [Read behavior (`EsReadConfig`)](#read-behavior-esreadconfig)

**You always create one of two config objects**, depending on the direction:

- **`EsWriteConfig`**: for `bulk_write` / `make_foreach_batch`.
- **`EsReadConfig`**: for `read_index`.

Both are frozen, serializable dataclasses (in `src/databricks_es_connector/config.py`) so they can
be shipped to Spark executors: the Elasticsearch client is built *inside* each partition, never on
the driver. Each one takes the same **connection fields** (hosts, auth, TLS, client tuning) plus the
fields specific to its direction. So a config is `connection + write behavior` or
`connection + read behavior`; the three tables below split exactly along that line.

> **`EsConfig` is an alias for `EsWriteConfig`**, so code using the older name keeps working. New
> code should use `EsWriteConfig`.

The package also exports, for direct use or testing: `read_coerce` (the declared-type read inverse)
and `sanitize_for_arrow` (Spark-side, called automatically by `bulk_write`). Plus `reconcile_or_raise`
and the exceptions `EsWriteError`, `ReadSchemaMismatch`.

## Connection (shared by both configs)

These fields are defined on the `EsConnection` base and accepted by both `EsWriteConfig` and
`EsReadConfig`.

| Field | Type | Default | Required | Notes |
|-------|------|---------|----------|-------|
| `hosts` | `str` | - | **Yes** | e.g. `"https://host:9200"`. Empty string raises `ValueError`. |
| `api_key` | `str \| None` | `None` | one-of\* | Preferred auth. Base64 `id:key` or an encoded key. |
| `basic_auth` | `tuple \| None` | `None` | one-of\* | `("user", "pass")`. Sandbox/dev only; prefer `api_key` in prod. |
| `verify_certs` | `bool` | `True` | No | Set `False` for self-signed sandbox boxes. Library default is secure. |
| `ca_certs` | `str \| None` | `None` | No | Path to a CA bundle when pinning. Mutually exclusive with `verify_certs=False`. |
| `http_compress` | `bool` | `True` | No | gzip both directions: compresses the request body on write and asks ES to gzip the response on read (`content-encoding` + `accept-encoding`). The egress-cost lever. |
| `request_timeout` | `int` | `60` | No | Per-request timeout (seconds). |
| `max_retries_per_request` | `int` | `3` | No | Retries for a failed whole **HTTP request**: a timeout, a connection that could not be made or was reset (TLS errors included), or an HTTP status in `request_retry_status_list` (e.g. a 429 when Elasticsearch's indexing pressure rejects the whole request). Re-sends the entire request, waiting with the backoff below. Applies to every Elasticsearch call the connector makes, reads and writes. `0` disables whole-request retries. Does **not** retry an individual rejected document, see [Retries](retries.md). |
| `max_retries` | `int` | - | No | Shorthand that sets **both** retry levels at once: `max_retries_per_request` and [`max_retries_per_doc`](#write-behavior-eswriteconfig). Cannot be combined with either of those (raises). Reading `cfg.max_retries` back gives the shared number, or `None` if the levels were set to different values. |
| `retry_on_timeout` | `bool` | `True` | No | Whether a request that exceeded `request_timeout` is retried. A timeout is ambiguous: the write may have been applied and only the response lost, so a re-sent write re-applies it (an idempotent upsert with `id_field`, a **duplicate** with auto-generated ids). Set `False` to fail a timed-out request on its first attempt; connection errors and `request_retry_status_list` statuses are still retried. |
| `retry_backoff_initial` | `float` | `2.0` | No | Seconds to wait before the first retry. Each later retry doubles the wait, up to `retry_backoff_max`. Applies to both retry levels. `0` retries immediately. |
| `retry_backoff_max` | `float` | `30.0` | No | Cap in seconds on any single retry wait. Must be `>= retry_backoff_initial`. Both backoff fields accept `0` to `86400` (one day). |
| `request_retry_status_list` | `tuple` | `(429, 502, 503, 504)` | No | HTTP statuses on a whole request that are retried. `429` is Elasticsearch rejecting the entire request (indexing pressure); `502`/`503`/`504` are a proxy or node that is briefly unavailable. Entries must be HTTP error statuses (`400`-`599`); empty means no status is retried. |

\* **Auth is required**: you must set exactly one of `api_key` or `basic_auth`, or the
constructor raises `ValueError`. Setting `ca_certs` together with `verify_certs=False` also
raises (pick one).

**How many retry levels apply depends on the direction.** A **read** has only the whole-request
level above, so `max_retries_per_request` and `max_retries` are equivalent there. A **write** has a
second, per-document level, and `max_retries=N` sets both at once, see
[Retries](retries.md).

## Write behavior (`EsWriteConfig`)

`EsWriteConfig` = the connection fields above **plus** these write-specific fields.

| Field | Type | Default | Required | Notes |
|-------|------|---------|----------|-------|
| `index` | `str` | `""` | **Yes** | Target index. `bulk_write` raises if empty. |
| `id_field` | `str \| None` | `None` | No | Column used as the deterministic `_id` → idempotent upserts. If unset, ES assigns random IDs, so a retry duplicates the row rather than overwriting it (at-least-once). This includes a Spark/stream replay **and**, within one write, any bulk in which some documents fail: the connector re-sends the whole bulk to classify the failures, re-creating the documents that had already succeeded in it. That covers a permanent rejection **and a transient, retryable `429` (routine ES backpressure)**, so under load an auto-id bulk that hits a single 429 is re-sent whole and its good documents are duplicated (with `id_field` set the re-send is an idempotent upsert instead). Set `id_field` for idempotent (exactly-once) upserts. If set, the column must **exist** (`bulk_write` raises before writing) and be non-null in every row. |
| `op_type` | `str` | `"index"` | No | The `_bulk` action for non-delete rows. `"index"` upserts by `_id` (a resend **overwrites** the existing doc). `"create"` appends only: ES answers a write whose `_id` already exists with a **409**, which the connector treats as an expected **no-op** (counted in `ignored`, surfaced as `docs_deduped` under `bulk_stats`) rather than an error, giving an **append-only** feed true idempotency on resend (a redelivered doc is neither overwritten nor duplicated) and letting ES take its cheaper append path (Lucene `addDocument`, no delete-by-`_id`). Use **only** where the feed never legitimately **updates** an existing `_id`: under `"create"` a real update is silently absorbed as a 409 no-op. Requires `id_field` (a create without an explicit `_id` never conflicts) and is incompatible with `has_deletes` (a feed that deletes is not append-only); auto-id feeds gain nothing (ES already appends known-unique ids) and must stay `"index"`. A 409 on an `"index"` op (an external version conflict) stays a hard error. **Count caveat:** under the default fast path, a chunk that mixes a brand-new `_id` with an already-existing one (or that hits a transient 429) creates the new doc on the probe, then re-ships the whole chunk where that new doc self-409s and is counted as `docs_deduped` rather than `written` (the doc is still correct in ES, present once; only the attribution is off). All-new and all-existing chunks count exactly. Set `bypass_fast_path=True` for exact counts. |
| `bypass_fast_path` | `bool` | `False` | No | Skip the `filter_path="errors"` fast-path probe and classify **every** chunk on the full per-item path (one send with a trimmed status/`_id` response, then a per-document 429 retry of only the rejected lines, with **no** whole-chunk re-ship). Default `False` keeps the fast path, whose GIL-avoidance (no per-item decode on a clean chunk) is the throughput win on wide/large writes. Set `True` when exact per-item accounting matters more than that decode saving, independent of `op_type`: for `op_type="create"` it makes `written`/`docs_deduped` exact on mixed new+existing chunks (see the `op_type` caveat) and avoids re-shipping conflict-heavy chunks at a busy ES; for auto-id `"index"` it avoids the documented at-least-once **duplication** a fast-path re-ship causes when a chunk that hit a 429 is re-sent whole. Costs per-item response decode on every chunk, including clean ones. |
| `chunk_size` | `int` | `500` | No | How many documents the connector groups into each `_bulk` HTTP request (the `N` in [Retries on a write](retries.md)). A chunk is also flushed early if it first reaches 100 MB (elasticsearch-py's `max_chunk_bytes` default, which matches Elasticsearch's own `http.max_content_length` request ceiling). Larger values mean fewer, bigger requests (higher throughput but more executor memory per request); smaller values mean more, smaller requests. `500` suits most workloads; lower it for very large documents. |
| `write_concurrency` | `int` | `1` | No | How many `_bulk` requests run **in parallel within a single partition**. `1` (default) ships a partition's chunks serially, so write concurrency across the cluster is just the partition count. Raise it when the write is **latency-bound** (executors idle waiting on each bulk's ES round-trip, with CPU *and* network both under-utilized) rather than CPU- or bandwidth-bound; each partition then keeps that many `chunk_size` sends **continuously in flight**, fed across Arrow-batch boundaries (a slow round-trip ties up one slot instead of stalling the whole partition), with the same per-document retry, so error accounting is unchanged. Per-send size is governed by `chunk_size` alone (bounded above by the Spark Arrow batch), independent of `write_concurrency`. Total requests hitting ES at once = (running partitions) × `write_concurrency`; raise it gradually and watch for 429s (if they climb, the ES cluster is the ceiling, not the client). Costs one executor thread per unit and up to `write_concurrency` chunks of in-flight docs per partition. |
| `max_retries_per_doc` | `int` | `3` | No | Retries for an individual document ES rejected with a retryable status (only the failed subset is re-sent), waiting with the same backoff as whole-request retries (`retry_backoff_initial` / `retry_backoff_max`). This is the knob that covers a per-document 429, which `max_retries_per_request` cannot see. |
| `retry_on_doc_status` | `tuple` | `(429,)` | No | Which per-document statuses to retry. `429` is ES's write queue being full, the one reliably transient case. Adding `503` (a shard briefly unavailable, e.g. during relocation) is the main sensible extension: `retry_on_doc_status=(429, 503)`. Do **not** add deterministic statuses like `400` (malformed doc) or `409` (version conflict): the retry fails identically and only delays the real error. Empty with a non-zero `max_retries_per_doc` raises. |
| `require_existing_index` | `bool` | `True` | No | Verify the index exists before writing. ES auto-creates a missing index, so a **typo'd index name** otherwise produces a brand-new dynamically-mapped index and a perfect-looking `written` count. One `indices.exists` call on the driver. Set `False` to allow auto-creation (e.g. with an index template). |
| `bulk_stats` | `bool` | `False` | No | Diagnostics: collect per-`_bulk` send stats and return them under an extra `bulk_stats` key in the write result (see [The write result](write-results.md)), one entry per write partition. Each carries `n_sends`, `docs_sent`, `bytes_sent`, `send_busy_ms`, `send_cpu_ms`, `partition_wall_ms`, the failed-send counters by kind (`timeout_*`, `connection_error_*`, `retry_status_*`, `error_*`, each `_sends`/`_wait_ms`) and `backoff_sleep_ms`, the retry/reject counters `docs_retried` (docs re-sent for a 429) and `rejected_429`/`rejected_409`/`rejected_4xx_other`/`rejected_5xx` (non-2xx item responses by status), the three nested round-trip distributions `rtt_ms_* ≥ http_ms_* ≥ took_ms_*` (mean/p50/p95/max), and `gil_wait_ms_*` + `gil_wait_samples`. `rtt − http` is the client layer above the node, `http − took` is compression + network, and `took` is ES service, so it answers whether a write is bound on ES, the network, client-side decode/GIL, or retries. `send_cpu_ms` plus the `gil_wait_ms_*` probe separate a slow round trip that is a genuine socket/ES wait from one inflated by GIL starvation under a high `write_concurrency` (see [The write result](write-results.md)). On the happy path it adds `took` to the `filter_path`, so the cost is one small extra timing per request plus a lightweight background probe thread; **off by default** (zero overhead and the result key absent when unset). |

How the retry fields work together is in [Retries](retries.md).

**Doc shaping**

| Field | Type | Default | Required | Notes |
|-------|------|---------|----------|-------|
| `drop_fields` | `tuple` | `()` | No | Columns the client chooses to **skip** to shrink payload (egress opt-out), e.g. `("raw_data", "unmapped")`. |
| `strict_drop_fields` | `bool` | `True` | No | Validate every `drop_fields` name against the DataFrame's columns and raise on an unknown one. A misspelled entry prunes **nothing**, shipping the field to ES while the caller believes it was withheld: since `drop_fields` is commonly a PII/egress control, it must fail **closed**. Set `False` only when deliberately reusing one config across DataFrames with differing columns. |

**Deletes**

| Field | Type | Default | Required | Notes |
|-------|------|---------|----------|-------|
| `has_deletes` | `bool` | `False` | No | `False` = every row is an index/upsert. `True` routes rows whose `delete_flag_column` is `true` to an ES delete-by-`_id`. |
| `delete_flag_column` | `str \| None` | `None` | when `has_deletes` | A **boolean** column: `true` deletes that `_id`, `false` or null indexes. The column is pruned from the `_source` of kept rows so it is never indexed. It must **exist** and be `BooleanType`: `bulk_write` raises before writing if it is missing or a non-boolean type (see below). |

> **A `delete_flag_column` that isn't in the DataFrame fails the write.** If the name is misspelled,
> every row reads as not-flagged, so each intended delete would be applied as an **upsert** and the
> documents would stay in Elasticsearch, reporting `deleted: 0`, `errors: 0` and a clean
> reconciliation. `bulk_write` therefore checks the column exists before writing anything. This is not
> optional: `has_deletes=True` has no meaning without a real flag column. (`id_field` and, under
> `strict_drop_fields`, `drop_fields` are validated in the same pass, for the same reason: a config
> field that names a column must fail closed when the name is wrong.)

> **The delete flag must be a real boolean.** Deletes are routed in Spark (`build_ndjson`) via
> `flag === true`, which has no way to parse a string or integer flag; a non-boolean column would
> compare to null for every row, route nothing to a delete, and silently upsert every intended
> deletion. `bulk_write` rejects a non-`BooleanType` flag column on the driver before writing. Cast
> the column in Spark first if needed: `df.withColumn("is_delete", col("is_delete").cast("boolean"))`.

When `has_deletes=True` the constructor requires both `id_field` (you cannot delete without an
`_id`) and `delete_flag_column`, and raises `ValueError` otherwise. Setting `delete_flag_column`
while `has_deletes=False` also raises: a flag column that does nothing is a misconfiguration, not
a silent no-op.

Delete idempotency: deleting a doc that isn't in ES returns a 404, which the connector treats as
an expected no-op (not an error), so checkpoint replays, filtered rows, and re-processed deletes
stay clean. This suppression is scoped strictly to `delete` + `404`; a 404 on an index, or any
other status on a delete, is still counted in `errors`.

See [Deletes](writing.md#deletes) for the recommended Change-Data-Feed pattern and the
cross-batch ordering caveat.

## Read behavior (`EsReadConfig`)

`EsReadConfig` = the connection fields above **plus** these read-specific fields.

| Field | Type | Default | Required | Notes |
|-------|------|---------|----------|-------|
| `index` | `str` | `""` | **Yes** | Source index. `read_index` raises if empty. |
| `id_field` | `str \| None` | `None` | No | If the declared schema names this column, it is filled from the ES `_id` when absent from `_source`. |
| `query` | `dict \| None` | `None` | No | Raw ES query DSL to filter the read. `None` = `match_all`. v1 accepts a raw DSL dict only, no Spark-predicate pushdown. (For `term` on string fields, note the [`.keyword` gotcha](reading.md#read_index).) |
| `num_slices` | `int \| None` | `None` | No | Parallelism for the distributed read. `None` defaults to the index's shard count. One slice per Spark task. |
| `strict_slices` | `bool` | `True` | No | When `num_slices` is `None` and the shard-count lookup fails (permissions, an alias spanning indices, a transient error), raise instead of degrading. The only correct fallback is a **single serial reader**: still complete, but it forfeits all parallelism, and a `logging` warning is easy to miss entirely on serverless, so a large export just looks mysteriously slow. Pass `num_slices` explicitly to skip the lookup, or set `False` to accept the serial read. |
| `batch_size` | `int` | `1000` | No | Docs per scroll/PIT page. Must be positive. |
| `pit_keep_alive` | `str` | `"5m"` | No | Point-in-Time lifetime, as a **sliding window**: each page re-sends it and resets the PIT's expiry, so it need only cover the longest gap between consecutive reads of the PIT (for `read_index`, the lazy open-to-first-page scheduling gap), not the whole job. An Elasticsearch time value (`d`, `h`, `m`, `s`, `ms`, `micros`, `nanos`, e.g. `"90s"`). It must be longer than the total backoff wait for one page (every wait `max_retries_per_request` allows; 14s with the defaults), or the constructor raises, because the PIT would expire while a page is retried. Each failed attempt also takes time (up to `request_timeout` for a timeout), so leave headroom above that total when you raise the retry settings. See [`read_index`](reading.md#read_index). |
| `include_id` | `bool` | `True` | No | Expose the ES `_id` via the `id_field` column when the schema declares it. |
