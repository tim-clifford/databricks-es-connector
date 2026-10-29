# Write Results and Diagnostics

What `bulk_write` returns, how to tell a complete write from one that lost or rejected rows, and the
opt-in `bulk_stats` diagnostics for a write that is slow or failing. The streaming entry point passes
the same dict to its `on_batch` callback (see
[Streaming](writing.md#streaming-make_foreach_batch)).

`bulk_write` returns a dict summarizing the write:

```python
{
  "written": 998,          # index/upsert ops that succeeded
  "deleted": 0,            # successful delete-by-id ops (non-zero only with has_deletes)
  "errors": 2,             # docs Elasticsearch rejected (exact count)
  "ignored": 0,            # expected no-ops: delete-404s, plus create-409 dedups under op_type="create"
  "coerced_nonfinite": 0,  # always 0 (see below): non-finite floats become null in Spark, uncounted
  "total_input": 1000,     # rows handed to the writer
  "unaccounted": 0,        # rows that produced NO per-document outcome (loss below that level)
  "overcounted": 0,        # more outcomes than input rows: impossible, so a bug in THIS library
  "error_samples": [       # bounded diagnostics for rejected docs (up to 20)
    {"_id": "abc", "op_type": "index", "status": 400,
     "reason": "failed to parse field [ts] of type [date]"},
  ],
}
```

- **`unaccounted` is the reconciliation check, pre-computed.** Every input row yields exactly one of
  `written`/`deleted`/`errors`/`ignored`, so any shortfall means rows vanished below the per-document
  level (e.g. a chunk-level serialization/transport error) where the `errors` count structurally
  cannot see them. `ignored` is part of the identity precisely so an expected no-op (a delete-404, or
  a create-409 dedup under `op_type="create"`) does not masquerade as loss. `bulk_write(df, cfg, raise_on_error=True)` (or `reconcile_or_raise(result)`)
  turns both signals into an `EsWriteError`; the streaming path does this by default.
- **`overcounted` should always be 0.** It counts the reverse discrepancy, more per-document outcomes
  than input rows, which is impossible by construction and so indicates a counting bug in this
  library rather than anything wrong with your data. It is reported and logged but does **not** fail
  the write, because failing a healthy write over our defect would wedge a streaming pipeline in a
  retry loop on a batch that can never pass. It is a separate field rather than a negative
  `unaccounted` so that it can never net against real loss: the two are accumulated per partition, so
  one partition over-counting cannot cancel out another partition genuinely losing rows. If you ever
  see a non-zero value, please report the result dict.
- **`coerced_nonfinite` is always `0`.** `inf`/`-inf`/`NaN` have no JSON representation (ES rejects
  the bare `Infinity`/`NaN` tokens), so the connector still turns them into JSON null at any nesting
  depth so the document is accepted, but this happens in Spark (`to_json` never sees a non-finite),
  off the per-row Python path that used to count them, so the field is not incremented. The key is
  kept in the result for shape stability. If you need to know a non-finite became null (e.g. to catch
  an upstream divide-by-zero), detect it in Spark before the write.
- **`error_samples` is a breadcrumb, not a dead-letter queue.** It retains up to the first 20
  failures (id, op, HTTP status, ES reason) so a failed write is diagnosable instead of an opaque
  count. The `errors` count is always exact; only the retained sample list is capped, so a batch
  that fails wholesale can't exhaust memory. If you need every failed row durably, capture them
  from your own pipeline, the connector does not persist them.
- **`bulk_stats` is opt-in diagnostics, absent by default.** With `EsWriteConfig(bulk_stats=True)`,
  the result carries an extra `bulk_stats` key: a list with one entry per write partition, each
  `{n_sends, docs_sent, bytes_sent, send_busy_ms, send_cpu_ms, partition_wall_ms, timeout_sends,
  timeout_wait_ms, connection_error_sends, connection_error_wait_ms, retry_status_sends,
  retry_status_wait_ms, error_sends, error_wait_ms, backoff_sleep_ms, docs_retried, rejected_429, rejected_409,
  rejected_4xx_other, rejected_5xx, docs_deduped, rtt_ms_mean/p50/p95/max, http_ms_mean/p50/p95/max,
  took_ms_mean/p50/p95/max, gil_wait_ms_total/p50/p95/max, gil_wait_samples}`.
  The round trip splits into three **nested** timings, `rtt_ms ≥ http_ms ≥ took_ms`: `rtt_ms` is the full
  client-observed `_bulk` wall; `http_ms` is `elastic_transport`'s node-level HTTP wall (request gzip +
  network + ES + response read); `took_ms` is Elasticsearch's own service time. So `rtt − http` is the
  client layer above the node (response decode + dispatch + GIL, what the fast path avoids), `http − took`
  is compression + network transit, and `took` is ES itself; `docs_sent / n_sends` is the real docs-per-bulk.
  `send_cpu_ms` and `gil_wait_ms_*` diagnose WHY a round trip is slow: `send_cpu_ms` is
  the CPU each worker actually burned inside `_bulk`, so `send_busy_ms − send_cpu_ms` is the off-CPU send
  time, and `gil_wait_ms_*` (from a background probe measuring how long a runnable thread waited to
  re-acquire the GIL) says whether that off-CPU time is a genuine socket/ES wait (`gil_wait_ms` near
  zero while `rtt_ms` is high) or client-side GIL starvation (`gil_wait_ms_total` a large share of
  `partition_wall_ms`, meaning a high `write_concurrency` is contending on the interpreter rather than
  overlapping I/O). All of those are over **successful** sends; the sends that **raised** are summarized
  by failure kind, each as a count and summed wall time: `timeout_sends`/`timeout_wait_ms` (a
  `ConnectionTimeout`), `connection_error_sends`/`connection_error_wait_ms` (connection refused or
  reset, DNS, TLS), `retry_status_sends`/`retry_status_wait_ms` (an HTTP status in
  [`request_retry_status_list`](configuration.md#connection-shared-by-both-configs), e.g. a whole-request 429 from
  indexing pressure) and `error_sends`/`error_wait_ms` (anything else, never retried). Total time in
  `_bulk` = `send_busy_ms` + the four `*_wait_ms`, and `backoff_sleep_ms` is the time spent waiting
  between retries, so none of it is hidden. `docs_retried` is the number of documents re-sent because
  of a retryable per-item **429** (the item-level analog of `retry_status_sends`), and `rejected_429` / `rejected_409` /
  `rejected_4xx_other` / `rejected_5xx` bucket every non-2xx, non-ignored item response by status (counted
  on every occurrence, so a 429 that later succeeds on retry still shows), which separates transient
  backpressure you can retry (429/503) from permanent rejections you cannot (400/409). `docs_deduped`
  counts create-409 append-only dedups (an `op_type="create"` resend whose `_id` already existed): a
  benign no-op, tracked separately from a genuine `rejected_409` conflict so you can watch how many
  resends a feed is absorbing. (Under the default fast path this over-counts a brand-new doc that shares
  a chunk with an existing one, since the whole-chunk re-ship self-409s it, at the expense of `written`;
  set `bypass_fast_path=True` for exact `written`/`docs_deduped`. See the `op_type` count caveat.) These
  are populated only on the full (errors) path, so a clean chunk contributes nothing. The key is **absent** when `bulk_stats` is off, so the core result shape above is
  unchanged.
- **Duplicate `id_field` values collapse, and reconciliation won't flag it.** If `id_field` is
  set and two input rows share the same id, the deterministic `_id` makes the later row **upsert
  over** the earlier one, so ES ends up with fewer documents than rows you sent. Every op reports
  success (`written` counts each op, not each surviving doc), so `written + deleted + errors` still
  equals `total_input`: the reconciliation identity passes even though the ES doc count is lower.
  This is the same idempotency that makes replays safe, but if you expect a 1:1 row→document
  mapping, ensure `id_field` is unique across the input (e.g. dedup upstream) or leave it unset to
  let ES assign random ids.
