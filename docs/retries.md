# Retries

The connector owns every retry. Each Elasticsearch client it builds has the transport's own retries
turned off, and every call (the bulk sends and index check on a write; the shard-count lookup, PIT
open and page requests on a read) is retried by the connector with exponential backoff. The transport
would re-send immediately, with no wait, which uses up the whole budget in milliseconds against a
cluster that is pushing back.

A bulk request sends N documents in one HTTP call, and ES answers with a **single HTTP 200** plus a
per-item status in the response *body*. "The request succeeded" and "the documents succeeded" are
therefore different questions, so a write has two retry layers:

| | [`max_retries_per_request`](configuration.md#connection-shared-by-both-configs) | [`max_retries_per_doc`](configuration.md#write-behavior-eswriteconfig) |
|---|---|---|
| Defined on | `EsConnection` | `EsWriteConfig` |
| Default | `3` | `3` |
| Retries on | A timeout (if `retry_on_timeout`), a connection error, or a status in `request_retry_status_list` (default `429, 502, 503, 504`) | A per-item status in `retry_on_doc_status` (default `429`) |
| Retry unit | The entire request, all N documents | Only the failed subset |
| Catches | Indexing pressure rejecting the whole request, ES unreachable, LB 503, gateway timeout, connection reset | A document ES refused because its write queue is full |

A **429 means different things at each layer**: "the cluster rejected your request" versus "the
cluster rejected this document". `max_retries_per_request` cannot see the second one, because the
per-item statuses live in the response body, not the HTTP status. That is why `max_retries_per_doc`
exists, and why leaving it at `0` while raising `max_retries_per_request` warns: rejected documents
would get no retries at all. Any other failure (a `400`, an auth error, a status not in the list) is
never retried: the chunk counts as errors on its first attempt.

**Setting them.** `max_retries=N` is shorthand for both levels at once:

```python
EsWriteConfig(..., max_retries=5)                                    # 5 at both levels
EsWriteConfig(..., max_retries_per_request=5, max_retries_per_doc=2)  # or set each
```

Use one style or the other; combining `max_retries` with either individual field raises, so the
effective value is never in doubt.

**Backoff.** Both layers wait before retry n for `min(retry_backoff_initial * 2**(n-1),
retry_backoff_max)` seconds, so the defaults wait 2s, 4s, 8s, 16s, then 30s for every later retry.
The wait blocks the executor thread that is sending, which is the point under indexing pressure: it
takes that thread's load off the cluster while it recovers. It also means a high retry count is slow
to give up: with the defaults, `max_retries=8` is up to 150s of waiting for one send. A chunk can make
several sends (the fast-path probe, the full-path re-send, and one per per-document retry round), each
with its own whole-request budget, so a chunk under sustained pressure can wait a multiple of that.

**Re-sent writes.** A whole-request retry re-sends the whole chunk. After a timeout, a connection that
dropped after the request was sent, or a `5xx`, the outcome of the first attempt is unknown, so the
re-send may re-apply documents that were already written: with `id_field` set that is an idempotent
upsert; with auto-generated ids it duplicates them (at-least-once). `retry_on_timeout=False` stops only
the timeout case.

**Several hosts.** With more than one entry in `hosts`, the client marks a node that fails to connect
as dead, so the connector's next attempt goes to a healthy node, after the backoff wait. On a read, a retried point-in-time open whose first response was lost can leave an
extra PIT open on the cluster until `pit_keep_alive` expires.
