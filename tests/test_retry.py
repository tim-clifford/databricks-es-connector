"""Unit tests for retry.py (connector-managed retries) and the retry fields on the config classes.

The write-path and read-path call sites are covered where those paths are tested
(test_spark_serialize.py, test_read.py); this file pins the shared pieces they all use.
"""
import pytest

from elasticsearch import ApiError, ConnectionError as EsConnectionError, ConnectionTimeout
from elastic_transport import ApiResponseMeta, HttpHeaders, SerializationError, TlsError

from databricks_es_connector import EsConnection, EsReadConfig, EsWriteConfig
from databricks_es_connector import retry


def _api_error(status):
    meta = ApiResponseMeta(status=status, http_version="1.1", headers=HttpHeaders({}), duration=0.0,
                           node=None)
    return ApiError(str(status), meta=meta, body={})


def _cfg(cls=EsWriteConfig, **kw):
    base = dict(hosts="https://h:9200", basic_auth=("u", "p"))
    if cls is not EsConnection:
        base["index"] = "i"
    base.update(kw)
    return cls(**base)


@pytest.fixture
def _slept(monkeypatch):
    waits = []
    import time
    monkeypatch.setattr(time, "sleep", lambda s: waits.append(s))
    return waits


# --- failure_kind / is_retryable ---------------------------------------------------------------

@pytest.mark.parametrize("exc, kind", [
    (ConnectionTimeout("t"), retry.TIMEOUT),
    (EsConnectionError("reset"), retry.CONNECTION_ERROR),
    (TlsError("handshake"), retry.CONNECTION_ERROR),          # TlsError subclasses ConnectionError
    (_api_error(429), retry.RETRY_STATUS),
    (_api_error(502), retry.RETRY_STATUS),
    (_api_error(503), retry.RETRY_STATUS),
    (_api_error(504), retry.RETRY_STATUS),
    (_api_error(400), retry.ERROR),
    (_api_error(404), retry.ERROR),
    (_api_error(500), retry.ERROR),
    (SerializationError("bad json"), retry.ERROR),
    (ValueError("bug"), retry.ERROR),
])
def test_failure_kind_classifies_with_the_default_status_list(exc, kind):
    assert retry.failure_kind(exc, _cfg()) == kind


def test_failure_kind_follows_the_configured_status_list():
    cfg = _cfg(request_retry_status_list=(500,))
    assert retry.failure_kind(_api_error(500), cfg) == retry.RETRY_STATUS
    assert retry.failure_kind(_api_error(429), cfg) == retry.ERROR
    assert retry.failure_kind(_api_error(429), _cfg(request_retry_status_list=())) == retry.ERROR


def test_is_retryable_allow_list():
    cfg = _cfg()
    assert retry.is_retryable(retry.TIMEOUT, cfg)
    assert retry.is_retryable(retry.CONNECTION_ERROR, cfg)
    assert retry.is_retryable(retry.RETRY_STATUS, cfg)
    assert not retry.is_retryable(retry.ERROR, cfg)
    assert not retry.is_retryable("something-new", cfg)       # unknown kinds fail closed
    assert not retry.is_retryable(retry.TIMEOUT, _cfg(retry_on_timeout=False))


# --- backoff_seconds ----------------------------------------------------------------------------

def test_backoff_defaults_double_from_2s_and_cap_at_30s():
    cfg = _cfg()
    assert [retry.backoff_seconds(n, cfg) for n in range(1, 8)] == [2, 4, 8, 16, 30, 30, 30]


def test_backoff_follows_initial_and_max():
    cfg = _cfg(retry_backoff_initial=0.5, retry_backoff_max=3)
    assert [retry.backoff_seconds(n, cfg) for n in range(1, 6)] == [0.5, 1, 2, 3, 3]


def test_backoff_initial_zero_means_immediate_retries():
    cfg = _cfg(retry_backoff_initial=0, retry_backoff_max=0)
    assert [retry.backoff_seconds(n, cfg) for n in range(1, 4)] == [0, 0, 0]


# --- call_with_retries --------------------------------------------------------------------------

class _Flaky:
    """Callable that raises each exception in `failures` once, in order, then returns `result`."""
    def __init__(self, failures, result="ok"):
        self.failures = list(failures)
        self.result = result
        self.calls = 0

    def __call__(self):
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return self.result


def test_call_with_retries_returns_after_retryable_failures(_slept):
    fn = _Flaky([_api_error(429), EsConnectionError("reset")])
    assert retry.call_with_retries(fn, _cfg()) == "ok"
    assert fn.calls == 3
    assert _slept == [2, 4]


def test_call_with_retries_reraises_the_last_failure_when_the_budget_is_spent(_slept):
    last = _api_error(503)
    fn = _Flaky([_api_error(503), _api_error(503), last])
    with pytest.raises(ApiError) as exc:
        retry.call_with_retries(fn, _cfg(max_retries_per_request=2))
    assert exc.value is last
    assert fn.calls == 3
    assert _slept == [2, 4]


def test_call_with_retries_reraises_a_non_retryable_failure_immediately(_slept):
    boom = _api_error(400)
    fn = _Flaky([boom])
    with pytest.raises(ApiError) as exc:
        retry.call_with_retries(fn, _cfg())
    assert exc.value is boom
    assert fn.calls == 1 and _slept == []


def test_call_with_retries_zero_budget_never_retries(_slept):
    fn = _Flaky([ConnectionTimeout("t")])
    with pytest.raises(ConnectionTimeout):
        retry.call_with_retries(fn, _cfg(max_retries_per_request=0))
    assert fn.calls == 1 and _slept == []


def test_call_with_retries_reports_each_wait(_slept):
    seen = []
    fn = _Flaky([ConnectionTimeout("t"), ConnectionTimeout("t")])
    retry.call_with_retries(fn, _cfg(retry_backoff_initial=1), on_backoff=seen.append)
    assert seen == [1, 2] == _slept


# --- config fields ------------------------------------------------------------------------------

@pytest.mark.parametrize("cls", [EsConnection, EsWriteConfig, EsReadConfig])
def test_retry_defaults_on_every_config_class(cls):
    cfg = _cfg(cls)
    assert cfg.max_retries_per_request == 3
    assert cfg.retry_on_timeout is True
    assert cfg.retry_backoff_initial == 2.0 and cfg.retry_backoff_max == 30.0
    assert cfg.request_retry_status_list == (429, 502, 503, 504)
    assert cfg.client_kwargs()["max_retries"] == 0          # the transport never retries


@pytest.mark.parametrize("cls", [EsConnection, EsWriteConfig, EsReadConfig])
def test_client_kwargs_carry_no_transport_retry_settings(cls):
    # Only max_retries=0 reaches the client; no retry_on_timeout / retry_on_status that could
    # re-enable transport retries if the client defaults ever changed.
    kw = _cfg(cls).client_kwargs()
    assert "retry_on_timeout" not in kw and "retry_on_status" not in kw


@pytest.mark.parametrize("old_name, value", [
    ("transport_max_retries", 3),
    ("retry_transport_timeout", True),
])
def test_removed_retry_names_are_rejected(old_name, value):
    with pytest.raises(TypeError, match=old_name):
        _cfg(**{old_name: value})


def test_status_list_is_stored_as_a_tuple():
    cfg = _cfg(request_retry_status_list=[429, 503])
    assert cfg.request_retry_status_list == (429, 503)
    hash(cfg)                                                # frozen config stays hashable


def test_empty_status_list_is_allowed():
    assert _cfg(request_retry_status_list=()).request_retry_status_list == ()


@pytest.mark.parametrize("bad", [
    (200,), (399,), (600,), (True,), ("429",), (429.0,), "429", 429,
])
def test_status_list_rejects_anything_but_http_error_statuses(bad):
    with pytest.raises(ValueError, match="request_retry_status_list"):
        _cfg(request_retry_status_list=bad)


@pytest.mark.parametrize("kw, field", [
    ({"max_retries_per_request": -1}, "max_retries_per_request"),
    ({"retry_backoff_initial": -1}, "retry_backoff_initial"),
    ({"retry_backoff_initial": 10, "retry_backoff_max": 5}, "retry_backoff_max"),
    ({"retry_backoff_initial": float("nan")}, "retry_backoff_initial"),
    ({"retry_backoff_max": float("nan")}, "retry_backoff_max"),
    ({"retry_backoff_max": float("inf")}, "retry_backoff_max"),
    ({"retry_backoff_initial": "2"}, "retry_backoff_initial"),
    ({"retry_backoff_initial": None}, "retry_backoff_initial"),
    ({"retry_backoff_initial": True}, "retry_backoff_initial"),
])
def test_retry_field_validation(kw, field):
    with pytest.raises(ValueError, match=field):
        _cfg(**kw)


def test_umbrella_sets_the_per_request_layer():
    cfg = _cfg(max_retries=6)
    assert cfg.max_retries_per_request == 6 and cfg.max_retries_per_doc == 6
    read = _cfg(EsReadConfig, max_retries=6)
    assert read.max_retries_per_request == 6


# --- EsReadConfig: retry waits must fit inside pit_keep_alive ------------------------------------

@pytest.mark.parametrize("value, seconds", [
    ("5m", 300), ("90s", 90), ("1h", 3600), ("2d", 172800), ("1500ms", 1.5), ("10micros", 1e-5),
    ("10nanos", 1e-8),
])
def test_es_duration_parses_every_es_time_unit(value, seconds):
    from databricks_es_connector.config import _es_duration_seconds
    assert _es_duration_seconds(value, "x") == pytest.approx(seconds)


@pytest.mark.parametrize("bad", ["5", "5 m", "5M", "1.5m", "-5m", "5w", "", None, 300])
def test_pit_keep_alive_must_be_an_es_time_value(bad):
    with pytest.raises(ValueError, match="pit_keep_alive"):
        _cfg(EsReadConfig, pit_keep_alive=bad)


def test_default_retry_waits_fit_inside_the_default_pit_keep_alive():
    cfg = _cfg(EsReadConfig)                     # 2 + 4 + 8 = 14s of waiting, well inside 5m
    assert cfg.pit_keep_alive == "5m"


def test_read_config_rejects_retry_waits_that_could_outlive_the_pit():
    # 10 retries at the default backoff wait 2+4+8+16+30*6 = 210s: fine for 5m, not for 3m.
    _cfg(EsReadConfig, max_retries_per_request=10)
    with pytest.raises(ValueError, match="pit_keep_alive"):
        _cfg(EsReadConfig, max_retries_per_request=10, pit_keep_alive="3m")
    with pytest.raises(ValueError, match="pit_keep_alive"):
        _cfg(EsReadConfig, retry_backoff_initial=60, retry_backoff_max=120, pit_keep_alive="2m")


def test_write_config_is_not_bound_by_pit_keep_alive():
    # Only reads hold a PIT; a write may wait as long as its settings allow.
    assert _cfg(EsWriteConfig, max_retries_per_request=50).max_retries_per_request == 50
