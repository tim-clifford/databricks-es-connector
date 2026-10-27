"""Connection + read/write configuration for the Elasticsearch connector.

Plain, serializable frozen dataclasses so they can be captured in a closure and shipped to Spark
executors (mapInPandas / foreachBatch) without dragging a live client. The Elasticsearch client is
constructed *inside* each partition from a config, never on the driver, so nothing non-serializable
crosses the wire.

Three types, sharing one connection base:
  - EsConnection: connection + client tuning (hosts, auth, TLS, timeouts, compression).
  - EsWriteConfig: connection + write behavior (index, id_field, chunking, deletes). Used by
                     bulk_write / make_foreach_batch.
  - EsReadConfig: connection + read behavior (index, query, slicing, paging). Used by read_index.

`EsConfig` is a backward-compatible alias for `EsWriteConfig` (the legacy name), so existing write
code keeps working; new code should name the read/write config explicitly.

RETRIES: ONE UMBRELLA OR TWO LAYERS
-----------------------------------
The connector owns every retry (see retry.py); the Elasticsearch client is always built with its own
transport retries disabled. Elasticsearch can fail a write in two independent places, so there are two
retry layers:

  max_retries_per_request  a whole HTTP REQUEST failed (timeout, connection refused or reset, or an
                           HTTP status in request_retry_status_list, e.g. a 429 from indexing
                           pressure). Re-sends the entire request. Applies to reads and writes.
                           -> retry.call_with_retries
  max_retries_per_doc      ONE DOCUMENT inside a successful request was rejected (429, ES write queue
                           full). Re-sends only the failed subset. Writes only.
                           -> bulk._ship_ndjson_chunk

They exist separately because the `_bulk` API answers **HTTP 200 even when documents inside it
fail**: the per-item statuses live in the response body, not the HTTP status. A 429 therefore means
different things at each layer.

Configure them either way, but not both ways at once:

    EsWriteConfig(..., max_retries=5)                 # UMBRELLA: 5 at every layer
    EsWriteConfig(..., max_retries_per_request=5,     # GRANULAR: tune each layer
                       max_retries_per_doc=2)

`max_retries` is a constructor-level convenience, not a stored field: it expands into the real
per-layer fields. Passing it together with either per-layer field raises, so the effective retry
count never depends on an invisible precedence rule. Reading `cfg.max_retries` gives the shared value
when the layers agree, and `None` when they were set granularly (there is no single number to report).

Both layers wait between attempts with the same exponential backoff,
`min(retry_backoff_initial * 2**(n-1), retry_backoff_max)` (defaults 2s, 4s, 8s, ... capped at 30s), and
the wait sleeps BLOCKINGLY in the executor thread. With the defaults, `max_retries=8` is up to 150s of
sleep per chunk at each layer (2 + 4 + 8 + 16 + 30 * 4), so raise it with that cost in mind.
"""
from __future__ import annotations

import functools
import inspect
import math
import re
import warnings
from dataclasses import dataclass, field, fields
from typing import Optional


# Upper bound on retry_backoff_initial / retry_backoff_max: one day. A single wait longer than that is a
# configuration mistake, and an absurd value (e.g. 1e308) would otherwise fail only at time.sleep().
MAX_RETRY_BACKOFF_SECONDS = 86400


@dataclass(frozen=True)
class EsConnection:
    """Connection + client tuning shared by reads and writes.

    The Elasticsearch client kwargs are built from this (on the executor), so every field here is a
    client-level concern. `hosts` is the only required field; auth (exactly one of api_key /
    basic_auth) is validated in __post_init__.
    """
    # --- connection ---
    hosts: str                              # e.g. "https://host:9200"
    api_key: Optional[str] = None           # preferred: base64 id:key or encoded key
    basic_auth: Optional[tuple] = None      # ("user", "pass"), sandbox only
    verify_certs: bool = True               # False for self-signed sandbox boxes
    ca_certs: Optional[str] = None          # path to CA bundle when pinning

    # --- client tuning (apply to both read and write requests) ---
    http_compress: bool = True              # gzip both ways: request body on write, ES response on read
    request_timeout: int = 60

    # --- retries (connector-managed; see retry.py) ---
    # Whole-REQUEST retries: a request that timed out, could not connect (refused, reset, DNS, TLS), or
    # answered an HTTP status in request_retry_status_list is re-sent in full, up to this many times,
    # waiting with the backoff below. Applies to every ES call the connector makes (writes, reads and
    # the driver-side checks). They do NOT retry an individual rejected document, because the `_bulk`
    # API answers HTTP 200 even when items inside it fail; per-document retries are
    # `EsWriteConfig.max_retries_per_doc`. 0 disables whole-request retries.
    #
    # `max_retries` is the UMBRELLA that sets both layers at once (see the module docstring). Leave
    # this field alone and set `max_retries` for one simple knob; set this one to tune the
    # whole-request layer independently. Setting both is rejected, so the effective value is never
    # ambiguous.
    max_retries_per_request: int = 3
    # Whether a request that exceeded request_timeout is retried. A timed-out request has an UNKNOWN
    # outcome, so a re-sent write re-applies it: an idempotent upsert with id_field set, a duplicate
    # with auto-generated ids (at-least-once). Set False to fail a timed-out request on its first
    # attempt instead.
    retry_on_timeout: bool = True
    # Backoff between retries, for both layers: retry n waits
    # min(retry_backoff_initial * 2**(n-1), retry_backoff_max) seconds, so the defaults give 2s, 4s,
    # 8s, ... capped at 30s. The wait blocks the executor thread that is sending.
    retry_backoff_initial: float = 2.0
    retry_backoff_max: float = 30.0
    # HTTP statuses on a whole request that are retried. 429 is Elasticsearch rejecting the entire
    # request (indexing pressure); 502/503/504 are a proxy or node that is briefly unavailable. Each
    # entry must be an HTTP error status (400-599). Empty means no status is retried (timeouts and
    # connection errors still are).
    request_retry_status_list: tuple = (429, 502, 503, 504)

    def __post_init__(self):
        if not self.hosts:
            raise ValueError("hosts is required")
        if self.api_key is None and self.basic_auth is None:
            raise ValueError("EsConfig requires either api_key or basic_auth")
        if self.verify_certs is False and self.ca_certs:
            raise ValueError("ca_certs is set but verify_certs is False, pick one")
        if self.max_retries_per_request < 0:
            raise ValueError("max_retries_per_request must be >= 0")
        for name in ("retry_backoff_initial", "retry_backoff_max"):
            v = getattr(self, name)
            # A real, finite, non-negative number. bool is an int subclass and NaN passes every `<`
            # comparison, so both are rejected explicitly rather than failing later at time.sleep().
            if (isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
                    or not 0 <= v <= MAX_RETRY_BACKOFF_SECONDS):
                raise ValueError(
                    f"{name} must be a number of seconds from 0 to {MAX_RETRY_BACKOFF_SECONDS}, got {v!r}")
        if self.retry_backoff_max < self.retry_backoff_initial:
            raise ValueError(
                f"retry_backoff_max ({self.retry_backoff_max}) must be >= retry_backoff_initial "
                f"({self.retry_backoff_initial})")
        statuses = self.request_retry_status_list
        if isinstance(statuses, (str, bytes)) or not hasattr(statuses, "__iter__"):
            raise ValueError(
                f"request_retry_status_list must be a list of HTTP status codes, got {statuses!r}")
        statuses = tuple(statuses)
        for s in statuses:
            # bool is an int subclass; True would silently mean status 1, so reject it explicitly.
            if isinstance(s, bool) or not isinstance(s, int) or not 400 <= s <= 599:
                raise ValueError(
                    f"request_retry_status_list entries must be HTTP error statuses (int 400-599), "
                    f"got {s!r}")
        # Stored as a tuple so the frozen config stays hashable whatever sequence the caller passed.
        object.__setattr__(self, "request_retry_status_list", statuses)

    def client_kwargs(self) -> dict:
        """Kwargs for elasticsearch.Elasticsearch(...). Built on the executor."""
        kw = {
            "hosts": self.hosts,
            "http_compress": self.http_compress,
            "request_timeout": self.request_timeout,
            # The transport never retries: every retry is connector-managed (retry.call_with_retries),
            # with backoff. Left on, the transport would re-send immediately and stack under ours.
            "max_retries": 0,
            "verify_certs": self.verify_certs,
        }
        if self.api_key is not None:
            kw["api_key"] = self.api_key
        if self.basic_auth is not None:
            kw["basic_auth"] = self.basic_auth
        if self.ca_certs is not None:
            kw["ca_certs"] = self.ca_certs
        return kw


@dataclass(frozen=True)
class EsWriteConfig(EsConnection):
    """Connection + write behavior for bulk_write / make_foreach_batch."""
    # --- write behavior ---
    index: str = ""                         # target index; one index per write (no per-row routing)
    id_field: Optional[str] = None          # column used as deterministic _id (idempotency)
    chunk_size: int = 500                   # docs per bulk request

    # How many `_bulk` requests run IN PARALLEL within a single DataFrame partition. 1 (default) ships
    # a partition's chunks serially, so write concurrency across the cluster is just the partition
    # count. Raise it when the write is LATENCY-bound: executors sit idle waiting on each bulk's ES
    # round-trip (CPU and network both under-utilized) rather than CPU- or bandwidth-bound. Each
    # partition then keeps `write_concurrency` bulk requests in flight at once, filling that wait: the
    # pre-built NDJSON is shipped through a bounded pool (bulk._PipelinedShipper) that keeps that many
    # sends in flight CONTINUOUSLY across Arrow batches, each a chunk_size send with the SAME
    # per-document 429 retry, so the error accounting is unchanged; only the number of concurrent
    # in-flight requests grows. Costs one executor thread and up to `write_concurrency` chunks of
    # in-flight docs per partition (the shipper's backpressure bound). Total requests hitting ES at once
    # = (running partitions) * write_concurrency, so raise it gradually and watch for 429s (ES write
    # queue full) - if they climb, the ES cluster, not the client, is the ceiling. The per-node HTTP
    # connection pool is sized to write_concurrency automatically (see client_kwargs), so callers do
    # not also have to raise it.
    write_concurrency: int = 1

    # Per-DOCUMENT retries for rows Elasticsearch rejects with a retryable status (429
    # es_rejected_execution_exception: the ES write queue is full). This is NOT the same as
    # EsConnection.max_retries_per_request, which fires on a failed whole REQUEST: the _bulk API
    # returns HTTP 200 even when individual items fail, so the whole-request retry never sees a
    # rejected document. Without this, a 429'd row is a hard error on its first and only attempt.
    # Waits with the same backoff as whole-request retries (retry_backoff_initial / retry_backoff_max).
    max_retries_per_doc: int = 3
    retry_on_doc_status: tuple = (429,)     # per-doc statuses worth retrying (429 = ES queue full)

    # Diagnostics: when True, time every individual `es.bulk` send and surface PER-PARTITION
    # aggregates (send count, docs/send, and client round-trip vs ES `took` distributions) on the
    # result dict under `bulk_stats`, so a caller can see where the DBR<->ES round trip goes without
    # a per-send row explosion. Off by default and zero-overhead when off (no timing, no extra
    # response field). When on, the fast path also requests `took` (filter_path="errors,took"), which
    # still omits the per-item array. See bulk._aggregate_bulk_stats / _merge_partition_results.
    bulk_stats: bool = False

    # --- doc shaping ---
    drop_fields: tuple = field(default_factory=tuple)  # columns to prune before indexing (egress lever)
    # drop_fields is frequently used as a PII/egress control, and a misspelled column name would
    # silently prune NOTHING (the field ships to ES anyway). True => bulk_write validates every
    # drop_fields name against the DataFrame schema and raises on an unknown one, so the control
    # fails CLOSED. Set False only if you deliberately reuse one config across DataFrames with
    # differing columns.
    strict_drop_fields: bool = True

    # Elasticsearch auto-creates a missing index on write (action.auto_create_index defaults to
    # true), so a TYPO'd index name silently produces a brand-new dynamically-mapped index and a
    # perfect-looking written count. True => bulk_write checks the index exists first and raises if
    # not. Set False for a pipeline that intentionally relies on auto-creation or an index template.
    require_existing_index: bool = True

    # --- deletes ---
    # has_deletes=False (default): every row is an index/upsert.
    # Set has_deletes=True *and* delete_flag_column to route rows whose flag is true to an ES
    # delete-by-id instead of an index. Requires id_field (you cannot delete without an _id). The flag
    # must be a real BooleanType column (a true row deletes; false or null indexes): deletes are routed
    # in Catalyst via `flag === true`, which cannot parse a string/int flag, so a non-boolean column is
    # rejected in bulk._preflight rather than silently upserting every intended delete.
    has_deletes: bool = False
    delete_flag_column: Optional[str] = None  # boolean column: true => delete this _id

    # --- write op type: index (default) vs create ---
    # The `_bulk` action used for every NON-delete row. "index" (default) upserts by _id, so a resend
    # overwrites the existing doc. "create" appends only: Elasticsearch rejects a write whose _id
    # already exists with a 409, which the connector then treats as an expected no-op - counted as
    # `ignored` (never an error) and surfaced distinctly as `docs_deduped` under bulk_stats. That gives
    # an APPEND-ONLY feed true idempotency on resend (a redelivered doc is neither overwritten, as
    # "index" would, nor duplicated) and lets Elasticsearch take its cheaper append path (Lucene
    # addDocument, no delete-by-_id term) instead of the update path.
    # Use ONLY for feeds that never legitimately UPDATE an existing _id: under "create" a real update to
    # an already-indexed doc is silently absorbed as a 409 no-op. Requires id_field (a create without an
    # explicit _id can never conflict, so it would be "index" with extra steps and no dedup) and is
    # incompatible with has_deletes (a feed that deletes is not append-only). Auto-id feeds gain nothing
    # here (ES already appends known-unique ids) and must stay "index". A 409 on an "index" op (an
    # external version conflict) stays a hard error; only create+409 is the benign no-op. See
    # bulk.classify_bulk_result and spark_serialize.build_ndjson.
    #
    # COUNT CAVEAT under the default fast path (bypass_fast_path=False): the fast path probes a chunk
    # with filter_path="errors" and, if ANY item failed, re-ships the WHOLE chunk to classify per item.
    # A chunk that mixes a brand-new _id with an already-existing one (or that hits a transient 429)
    # therefore creates the new doc on the probe, then RE-creates it on the re-ship, where it now
    # self-409s and is classified as a dedup. The document is still correct in ES (present once, no
    # duplicate, no overwrite, reconcile balances), but `written` UNDER-counts it and `docs_deduped`
    # OVER-counts it for that chunk. The miscount is confined to chunks that straddle new + existing
    # docs; all-new and all-existing chunks count exactly. Set bypass_fast_path=True for exact counts
    # (and to skip the wasteful whole-chunk re-ship) at the cost of per-item decode on every chunk.
    op_type: str = "index"

    # Skip the filter_path="errors" fast-path probe and classify every chunk on the full per-item path
    # (a single send with a trimmed status/_id response, then a per-document 429 retry of only the
    # rejected lines - no whole-chunk re-ship). Default False keeps the fast path, whose GIL-avoidance
    # (no per-item decode on a clean chunk) is the throughput win on wide/large writes. Set True when
    # exact per-item accounting matters more than that decode saving, independent of op_type:
    #   - op_type="create": makes `written` / `docs_deduped` exact on mixed new+existing chunks (see the
    #     count caveat on op_type) and avoids re-shipping conflict-heavy chunks at an already-busy ES.
    #   - op_type="index" auto-id: avoids the documented at-least-once DUPLICATION a fast-path re-ship
    #     causes when a chunk that hit a 429 is re-sent whole (the full path retries only the 429'd line).
    # Costs per-item response decode on every chunk (including clean ones), which the fast path avoids.
    bypass_fast_path: bool = False

    def __post_init__(self):
        super().__post_init__()
        if self.max_retries_per_doc < 0:
            raise ValueError("EsWriteConfig.max_retries_per_doc must be >= 0")
        if self.write_concurrency < 1:
            raise ValueError("EsWriteConfig.write_concurrency must be >= 1")
        # The `max_retries` umbrella cannot produce this combination (it sets both layers to the same
        # value), so reaching here means the layers were configured GRANULARLY: the whole-request
        # layer was hardened while per-document retries were switched off. That is almost certainly not
        # what someone protecting against ES backpressure intended, because per-document retries are
        # the only ones that cover a 429'd document (the _bulk API returns HTTP 200 even when items
        # inside it fail, so the whole-request retry never sees them). Warn rather than raise: the
        # combination is legal and might be deliberate, just very unlikely.
        if self.max_retries_per_doc == 0 and self.max_retries_per_request > 3:
            warnings.warn(
                f"max_retries_per_request={self.max_retries_per_request} is raised above the default "
                "but max_retries_per_doc=0, so documents Elasticsearch rejects with a 429 get NO "
                "retries. The _bulk API returns HTTP 200 even when individual items fail, so "
                "whole-request retries never see them. Set max_retries_per_doc, or use "
                "max_retries=<n> to set both layers at once.",
                # stacklevel=4, not 3: __post_init__ <- generated __init__ <- the umbrella wrapper
                # <- the caller. The wrapper added a frame, so 3 pointed inside config.py, which
                # blames the library for the caller's configuration. Pinned by a test that asserts
                # the warning's filename/lineno, since this is exactly the kind of off-by-one that
                # silently returns if the call chain changes again.
                UserWarning, stacklevel=4)
        if not self.retry_on_doc_status and self.max_retries_per_doc:
            # Retries requested but no status to retry on would silently never retry.
            raise ValueError("max_retries_per_doc is set but retry_on_doc_status is empty, "
                             "name at least one status (e.g. (429,)) or set max_retries_per_doc=0")
        # Delete routing is all-or-nothing and needs an _id to target.
        if self.has_deletes:
            if not self.delete_flag_column:
                raise ValueError("has_deletes=True requires delete_flag_column")
            if self.id_field is None:
                raise ValueError("has_deletes=True requires id_field (deletes target a doc _id)")
        elif self.delete_flag_column is not None:
            # A flag column set with deletes off would silently do nothing, reject the misconfig
            # rather than let a caller believe deletes are happening.
            raise ValueError("delete_flag_column is set but has_deletes is False, enable has_deletes or drop it")
        # The remaining delete requirement - delete_flag_column must be a real BooleanType column, so
        # build_ndjson can route it in Catalyst via `flag === true` - needs the DataFrame schema, so
        # it is enforced in bulk._preflight (driver-side, once), not here where only field values exist.
        # op_type governs the non-delete action. Allow-list it (a typo fails closed here rather than
        # emitting a bogus _bulk action ES rejects wholesale), and enforce the two invariants that make
        # "create" both meaningful and safe.
        if self.op_type not in ("index", "create"):
            raise ValueError(f"op_type must be 'index' or 'create', got {self.op_type!r}")
        if self.op_type == "create":
            if self.id_field is None:
                raise ValueError(
                    "op_type='create' requires id_field: without an explicit _id a create never "
                    "conflicts, so it would be 'index' with extra steps and no dedup. Set id_field, or "
                    "use op_type='index'.")
            if self.has_deletes:
                raise ValueError(
                    "op_type='create' is incompatible with has_deletes: a feed that issues deletes is "
                    "not append-only. Use op_type='index' for delete-bearing feeds.")

    def client_kwargs(self) -> dict:
        """EsConnection.client_kwargs plus a per-node connection pool sized to write_concurrency.

        Each partition's ES client is shared by `write_concurrency` worker threads
        (bulk._PipelinedShipper). elastic_transport's per-node pool defaults to ~10 connections, so a
        higher write_concurrency would silently cap the in-flight requests below the configured value;
        sizing the pool to the concurrency gives every worker its own connection. Left at the client
        default for write_concurrency == 1 (the serial path), so nothing changes there.
        """
        kw = super().client_kwargs()
        if self.write_concurrency > 1:
            kw["connections_per_node"] = self.write_concurrency
        return kw


@dataclass(frozen=True)
class EsReadConfig(EsConnection):
    """Connection + read behavior for read_index.

    The caller declares the Spark schema separately (no mapping inference); this config carries
    only connection + which index / query / how to page and slice it.
    """
    index: str = ""                         # source index (required by read_index)
    id_field: Optional[str] = None          # if the declared schema names this, it is filled from _id
    query: Optional[dict] = None            # raw ES query DSL; None => match_all. No Spark pushdown in v1.
    num_slices: Optional[int] = None        # distributed-reader parallelism (None => shard count)
    batch_size: int = 1000                  # docs per scroll/PIT page
    # Point-in-Time lifetime. This is a SLIDING window, not a total budget: every page request
    # re-sends keep_alive and resets the PIT's clock (verified against the ES PIT API), so it only
    # has to cover the longest gap between consecutive touches of the PIT, not the whole job. The
    # binding gap for read_index is open-PIT -> first executor page (the DataFrame is lazy, so Spark
    # may not schedule the read tasks immediately, and serverless executor cold-start adds to it).
    # Default 5m gives that gap headroom; raise it if a slow downstream consumer stretches the gap
    # between pages. See read_index's docstring.
    pit_keep_alive: str = "5m"
    include_id: bool = True                 # expose ES _id when the schema declares the id_field
    # When num_slices is None the reader looks up the index's shard count. If that lookup fails it
    # can only fall back to ONE unsliced reader: correct but serial, silently forfeiting the
    # parallelism read_index exists for (and a `logging` warning is easy to miss entirely on
    # serverless). True => raise instead of degrading quietly. Set False to accept the serial read.
    strict_slices: bool = True

    def __post_init__(self):
        super().__post_init__()
        if self.batch_size <= 0:
            raise ValueError("EsReadConfig.batch_size must be positive")
        if self.num_slices is not None and self.num_slices < 1:
            raise ValueError("EsReadConfig.num_slices must be >= 1 when set")
        # A failed page is retried, and the PIT is only kept alive by touching it. If the backoff waits
        # for one page alone reached pit_keep_alive, the PIT would certainly expire mid-retry (the next
        # attempt gets a 404, not retryable, failing the read), so reject that configuration up front.
        # The failed attempts themselves also take time (up to request_timeout each, for a timeout), but
        # that risk exists without any retry (one slow request can outlive a short keep-alive), so it is
        # documented rather than enforced: enforcing it would reject the common pit_keep_alive="1m" at the
        # default request_timeout of 60s.
        from .retry import total_backoff_seconds
        keep_alive_s = _es_duration_seconds(self.pit_keep_alive, "EsReadConfig.pit_keep_alive")
        wait_s = total_backoff_seconds(self.max_retries_per_request, self)
        if wait_s >= keep_alive_s:
            raise ValueError(
                f"the retry backoff for one page ({wait_s:g}s: max_retries_per_request="
                f"{self.max_retries_per_request}, retry_backoff_initial={self.retry_backoff_initial}, "
                f"retry_backoff_max={self.retry_backoff_max}) is not below pit_keep_alive="
                f"{self.pit_keep_alive!r} ({keep_alive_s:g}s), so the point-in-time would expire while a "
                "page is being retried. Raise pit_keep_alive or lower the retry settings.")


# Elasticsearch's time units, in seconds. An allow-list: any other unit is rejected rather than guessed.
_ES_TIME_UNITS = {"d": 86400.0, "h": 3600.0, "m": 60.0, "s": 1.0, "ms": 1e-3, "micros": 1e-6,
                  "nanos": 1e-9}
_ES_DURATION_RE = re.compile(r"(\d+)(d|h|m|s|ms|micros|nanos)")


def _es_duration_seconds(value, where: str) -> float:
    """Seconds in an Elasticsearch time value such as "5m" or "90s"; raises ValueError on anything else.
    Trimmed and lowercased first, as Elasticsearch does, so "5M" is accepted like ES accepts it."""
    m = _ES_DURATION_RE.fullmatch(value.strip().lower()) if isinstance(value, str) else None
    if m is None:
        raise ValueError(f"{where} must be an Elasticsearch time value such as '5m' or '90s', got {value!r}")
    return int(m.group(1)) * _ES_TIME_UNITS[m.group(2)]


# The per-layer fields `max_retries` fans out to. Only the ones a class actually HAS are set, so the
# umbrella works on EsReadConfig (no per-document layer exists for a read) as well as write configs.
_RETRY_LAYER_FIELDS = ("max_retries_per_request", "max_retries_per_doc")


def _support_max_retries_umbrella(cls):
    """Give `cls` a `max_retries=` constructor argument that sets EVERY retry layer at once.

    Two ways to configure retries, and you pick one:

      EsWriteConfig(..., max_retries=5)                  # simple: 5 at both layers
      EsWriteConfig(..., max_retries_per_request=5,      # granular: tune each layer
                         max_retries_per_doc=2)

    Mixing them is REJECTED rather than resolved by precedence. If `max_retries=5` and
    `max_retries_per_doc=2` were both accepted, the effective per-document value would depend on an
    ordering rule nobody can see at the call site, which is exactly the class of ambiguity this
    config keeps trying to eliminate. An error naming both spellings is unmissable.

    `max_retries` is not a dataclass field: it is a pure constructor-level convenience that expands
    into the real fields. So `cfg.max_retries` is a read-only property (see below) rather than stored
    state, and there is never a stored value that could disagree with the layers it set.

    Applied AFTER @dataclass so it wraps the GENERATED __init__. Defining __init__ in the class body
    would make @dataclass skip generating one at all, leaving no `__dataclass_init__` to delegate to
    (verified). Wrapping keeps field order, defaults, inheritance, pickling and frozen-ness intact.
    """
    original = cls.__init__
    own_fields = {f.name for f in fields(cls)}
    layers = tuple(f for f in _RETRY_LAYER_FIELDS if f in own_fields)

    @functools.wraps(original)
    def __init__(self, *args, **kwargs):
        if "max_retries" in kwargs:
            conflicts = sorted(f for f in layers if f in kwargs)
            if conflicts:
                raise ValueError(
                    f"pass either max_retries (the umbrella, which sets {' and '.join(layers)}) or "
                    f"the per-layer field(s) {', '.join(conflicts)}, not both. Mixing them would "
                    "make the effective retry count depend on an invisible precedence rule. "
                    f"For {cls.__name__}, max_retries={kwargs['max_retries']!r} is equivalent to "
                    + ", ".join(f"{f}={kwargs['max_retries']!r}" for f in layers) + ".")
            umbrella = kwargs.pop("max_retries")
            for f in layers:
                kwargs[f] = umbrella
        original(self, *args, **kwargs)

    # Advertise `max_retries` in the signature. functools.wraps copies the GENERATED __init__'s
    # signature, which does not mention `max_retries` (it is not a field), so `help(EsWriteConfig)`,
    # IDE completion and anything else using inspect.signature would not reveal a supported argument.
    # Append it as keyword-only so introspection matches what the constructor actually accepts.
    try:
        _sig = inspect.signature(original)
        _params = list(_sig.parameters.values())
        _params.append(inspect.Parameter("max_retries", inspect.Parameter.KEYWORD_ONLY,
                                         annotation=int))
        __init__.__signature__ = _sig.replace(parameters=_params)
    except (TypeError, ValueError):  # pragma: no cover - introspection is best-effort, never fatal
        pass

    cls.__init__ = __init__

    def _max_retries(self):
        """The umbrella retry setting, if every layer currently agrees; else None.

        Read-only and derived, because `max_retries` is a constructor convenience rather than stored
        state. It returns None under a granular configuration (the layers differ), which is honest:
        there is no single number that describes it.
        """
        values = {getattr(self, f) for f in layers}
        return next(iter(values)) if len(values) == 1 else None

    cls.max_retries = property(_max_retries)
    return cls


# Each class is wrapped independently, because @dataclass regenerates __init__ for every subclass, so
# wrapping only the base would leave the subclasses without the umbrella.
#
# KNOWN BOUNDARY: a FURTHER subclass defined by a caller (e.g. `@dataclass(frozen=True) class
# MyConfig(EsWriteConfig)`) regenerates __init__ again and is not wrapped, so `max_retries=` raises
# TypeError there while the per-layer fields keep working. That fails loudly rather than silently
# mis-configuring retries, and subclassing these configs is not part of the public API (construct
# them, don't extend them), so it is left as-is rather than solved with metaclass machinery.
# `_support_max_retries_umbrella` is importable if a caller genuinely needs it on their own subclass.
for _cls in (EsConnection, EsWriteConfig, EsReadConfig):
    _support_max_retries_umbrella(_cls)


# Backward-compatible alias: the write config was originally named EsConfig. Keep it working so
# existing write code and tests don't break; new code should use EsWriteConfig / EsReadConfig.
EsConfig = EsWriteConfig
