"""Connector-managed retries for every Elasticsearch call the connector makes.

The connector owns ALL whole-request retries. Every client is built with the transport's own retries
disabled (`max_retries=0`, see EsConnection.client_kwargs), and each call site wraps its ES call in
`call_with_retries`: the write path's `es.bulk` and `indices.exists` preflight, and the read path's
`indices.get_settings`, `open_point_in_time` and per-page `es.search`. The transport's retries fire
immediately (with a single host there is no node to fail over to, so nothing waits), which burns the
whole budget in milliseconds against a cluster that is pushing back. Here every retry waits with
exponential backoff instead.

What is retried is an ALLOW-LIST; every other exception propagates on its first attempt:

  - ConnectionTimeout, when `retry_on_timeout` is True (the default). A timed-out request has an
    UNKNOWN outcome, so a re-sent write re-applies it: an idempotent upsert with id_field set, a
    duplicate with auto-generated ids (at-least-once).
  - ConnectionError, including its TlsError subclass (connection refused or reset, DNS, TLS).
  - ApiError whose HTTP status is in `request_retry_status_list` (default 429, 502, 503, 504): a
    whole-request 429 is Elasticsearch indexing pressure rejecting the entire request.

Per-DOCUMENT retries (`max_retries_per_doc`, inside a `_bulk` response that answered HTTP 200) are a
separate loop in bulk._ship_ndjson_chunk; it uses the same `backoff_seconds` wait.

The wait before retry n (1-based) is `min(retry_backoff_initial * 2**(n-1), retry_backoff_max)`, so the
defaults give 2s, 4s, 8s, ... capped at 30s. Pure and Spark-free so it is unit-testable.
"""
from __future__ import annotations

import time
from typing import Callable, Optional

# Failure kinds. Also the bulk_stats outcome label of a failed send (see bulk._aggregate_bulk_stats).
TIMEOUT = "timeout"
CONNECTION_ERROR = "connection_error"
RETRY_STATUS = "retry_status"
ERROR = "error"


def _status(exc) -> Optional[int]:
    """The HTTP status of an ApiError (elasticsearch-py 8 keeps it on `.meta.status`), else None."""
    return getattr(getattr(exc, "meta", None), "status", None)


def failure_kind(exc, cfg) -> str:
    """Classify a raised exception into one of the failure kinds above.

    ConnectionTimeout is checked first: in elastic_transport it is a sibling of ConnectionError, not a
    subclass, but checking it first keeps the timeout kind exact if that ever changes. TlsError IS a
    ConnectionError subclass and so classifies as CONNECTION_ERROR. An ApiError is RETRY_STATUS only when
    its status is in `cfg.request_retry_status_list`; any other status, and any other exception, is ERROR.
    """
    from elasticsearch import ApiError, ConnectionError as _ConnectionError, ConnectionTimeout
    if isinstance(exc, ConnectionTimeout):
        return TIMEOUT
    if isinstance(exc, _ConnectionError):
        return CONNECTION_ERROR
    if isinstance(exc, ApiError) and _status(exc) in cfg.request_retry_status_list:
        return RETRY_STATUS
    return ERROR


def is_retryable(kind: str, cfg) -> bool:
    """Whether a failure of `kind` may be retried under `cfg`. ERROR never is."""
    if kind == TIMEOUT:
        return bool(cfg.retry_on_timeout)
    return kind in (CONNECTION_ERROR, RETRY_STATUS)


def backoff_seconds(attempt: int, cfg) -> float:
    """Seconds to wait before retry number `attempt` (1-based): exponential from
    `cfg.retry_backoff_initial`, capped at `cfg.retry_backoff_max`. The exponent is clamped so a very
    high attempt number saturates at the cap instead of overflowing the float conversion."""
    return min(cfg.retry_backoff_initial * 2.0 ** min(attempt - 1, 1023), cfg.retry_backoff_max)


def total_backoff_seconds(retries: int, cfg) -> float:
    """The summed wait across `retries` retries, i.e. backoff_seconds(1..retries). Once the wait reaches
    the cap every later retry waits the cap, so this stops iterating there (constant time for any
    budget)."""
    total = 0.0
    for n in range(1, retries + 1):
        wait = backoff_seconds(n, cfg)
        if wait >= cfg.retry_backoff_max:
            return total + (retries - n + 1) * cfg.retry_backoff_max
        total += wait
    return total


def call_with_retries(fn: Callable, cfg, on_backoff: Optional[Callable[[float], None]] = None):
    """Call `fn()` and return its result, retrying a retryable failure up to
    `cfg.max_retries_per_request` times with backoff. A non-retryable failure, or the last failure once
    the budget is spent, is re-raised unchanged so the caller's own error handling sees it.

    `on_backoff(seconds)` is called after each wait, so a caller can record the time spent waiting (the
    write path sums it into bulk_stats `backoff_sleep_ms`).
    """
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - classified below; anything not allow-listed re-raises
            if attempt >= cfg.max_retries_per_request or not is_retryable(failure_kind(exc, cfg), cfg):
                raise
            attempt += 1
            wait = backoff_seconds(attempt, cfg)
            time.sleep(wait)
            if on_backoff is not None:
                on_backoff(wait)
