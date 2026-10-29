# databricks-es-connector

Serverless-safe, bi-directional transfer between Databricks/Spark and Elasticsearch.

Python library. **Write:** given a Spark DataFrame and an `EsWriteConfig`, it writes rows to an
Elasticsearch index via the `elasticsearch-py` client, parallelized across executors with
`mapInPandas` (serverless-safe), with gzip request compression and deterministic document IDs for
idempotent upserts. The writer is schema-agnostic: hand it any DataFrame, no pre-processing.
**Read:** given an `EsReadConfig` and a *declared* Spark schema (the reader does not infer one),
`read_index` pulls an index back into a DataFrame, distributed across executors via a sliced
Point-in-Time scroll.

Built because the `elasticsearch-spark` connector cannot run on serverless compute
(no third-party Spark JARs), has no Spark 4 / DBR 17+ build, and offers no request
compression. The Python client has none of those limits.

> **Maturity.** The mechanism is proven for both directions: every valid Spark datatype is
> exportable with no caller pre-processing (inserts, upserts, deletes), and an index can be read back
> into a DataFrame against a declared schema with write→read fidelity (see
> [Datatype Fidelity](docs/datatypes.md)). Production hardening items (TLS/CA trust, API-key auth
> worked example, FIPS/FedRAMP, error dead-lettering, index templates/ILM) are not yet built. See
> [Production Readiness](docs/production-readiness.md) for the known limitations and pre-production checklist.

## Why this shape

**Shared by both directions:**

- **Serverless works.** Both the write and the read path use `mapInPandas`, not RDD APIs
  (`foreachPartition` and custom `DataSource`/RDD readers are blocked on serverless). Work still
  fans out across executors: writes partition the DataFrame, reads fan out one task per index slice.
- **Egress-minimized.** `http_compress=True` gzips both directions (~5-10x on JSON): the request
  body on write, and the ES response on read. A lever for cross-cloud cost either way.

**Writing** (`bulk_write` / `make_foreach_batch`):

- **Idempotent.** Deterministic `_id` (from a configurable column) means checkpoint
  replays and backfills upsert instead of duplicating.
- **Field pruning.** `drop_fields` drops columns you don't need indexed before the write, a further
  write-side egress lever on top of compression.
- **Schema-agnostic on write.** The writer knows nothing about your schema or data model, you
  hand it any DataFrame and a target index, and every Spark datatype is exportable with no
  pre-processing (see [Datatype Fidelity](docs/datatypes.md)).

**Reading** (`read_index`):

- **Distributed snapshot read.** One Elasticsearch Point-in-Time gives a consistent snapshot, and a
  sliced scroll fans out across executors (one task per shard/slice) so a full-index export scales.
- **Explicit schema required.** Unlike the writer, the reader must be handed a Spark schema: several
  stored values are ambiguous and can't be inverted from `_source` alone, so the caller declares the
  intended types. This is the deliberate trade-off for exact write→read fidelity; see
  [Declaring the schema](docs/reading.md#declaring-the-schema).

## Documentation

| If you want to | Read |
|---|---|
| Install the connector in a workspace and store the Elasticsearch credentials | [Installation and Setup](docs/setup.md) |
| Write a DataFrame and read it back, as a first test | [Quick Start](docs/quick-start.md) |
| Write in batch or streaming, handle a failed write, or propagate deletes | [Writing to Elasticsearch](docs/writing.md) |
| Check a write's result, or diagnose a slow or failing write with `bulk_stats` | [Write Results and Diagnostics](docs/write-results.md) |
| Read an index into a DataFrame and declare its schema | [Reading from Elasticsearch](docs/reading.md) |
| Know how each Spark type is stored in Elasticsearch and read back | [Datatype Fidelity](docs/datatypes.md) |
| Look up any config field: type, default, what it does | [Configuration Reference](docs/configuration.md) |
| Understand the two retry levels and the backoff | [Retries](docs/retries.md) |
| Check what a production deployment still needs | [Production Readiness / Known Limitations](docs/production-readiness.md) |
| Change the library, run the unit tests, or build the wheel | [Developing the Connector](docs/development.md) |
| Run the live integration tests on Databricks serverless | [Integration Tests](docs/integration-tests.md) |
| Cut a tagged release | [Releasing](docs/releasing.md) |

## Layout

```
src/databricks_es_connector/   # the library (this is what ships in the .whl)
  config.py                    #   EsConnection base + EsWriteConfig / EsReadConfig (EsConfig alias)
  spark_prep.py                #   sanitize_for_arrow (Arrow-hostile VARIANT/INTERVAL -> JSON string) + timestamp -> epoch-millis normalization
  spark_serialize.py           #   build_ndjson: build the whole _bulk NDJSON in Spark (to_json), the WRITE serializer
  bulk.py                      #   executor-side mapInPandas bulk write: ship the NDJSON, classify + reconcile (batch entry point)
  stream.py                    #   foreachBatch helper for Structured Streaming
  read_transform.py            #   pure-Python inverse coercion on READ (ES value + type -> Spark value)
  read.py                      #   read_index (distributed sliced-scroll PIT reader)
  retry.py                     #   connector-managed retries with backoff, shared by every ES call
tests/                         # unit tests for the pure-Python layer (no Spark/ES needed)
integration_tests/             # live-Spark/ES tests run on Databricks serverless via dbx_test
  test_datatype_coverage.py    #   every Spark datatype + edge cases, round-tripped through ES
  test_bulk_write_roundtrip.py #   the bulk_write result contract (counts, total_input, error_samples)
  test_autoid_fast_path.py     #   auto-id (no id_field) fast path: clean = no dup, chunk w/ a reject re-ships (dup)
  test_create_append_only.py   #   op_type="create": resend of an existing _id dedups (409, no dup, no overwrite)
  test_concurrency_roundtrip.py #  write_concurrency>1: threaded per-partition write, no loss/dupe/miscount
  test_deletes_roundtrip.py    #   has_deletes routing live: delete-by-id, delete-404 no-op
  test_preflight_column_guards.py # a config field naming a missing column fails before any write
  test_streaming_sink.py       #   make_foreach_batch on real Structured Streaming + restart idempotency
  test_read_roundtrip.py       #   write->read fidelity + distributed sliced read (multi-shard)
  test_timezone_utc.py         #   timestamp epoch is session-timezone-independent (UTC + non-UTC)
  test_dynamic_mapping_coercion.py #  ES dynamic-mapping coercion: _source faithful vs indexed value
  test_sanitize_for_arrow.py   #   the Spark-side VARIANT/INTERVAL serialization, no ES
  config/test_config.yml       #   dbx_test config (profile, wheel, serverless env)
scripts/                       # release + maintenance gates (not shipped in the .whl)
  check_requirements_match.py  #   requirements.txt == the wheel's resolved dependency closure
  check_readme_sync.py         #   every module / fixture / script is documented in the README(s)
  check_version_consistency.py #   __init__, HANDOFF header + every wheel reference match pyproject
.agents/skills/es-connector/   # maintainer skill: fidelity model, datatype contract, ES gotchas, release
docs/                          # the documentation above (README.md is the entry point)
```

Two test tiers: `tests/` is the fast, infra-free gate (pure-Python, run with `pytest`);
`integration_tests/` runs on a live serverless workspace + Elasticsearch to cover the Spark-side
code the unit tests can't reach, see [Integration Tests](docs/integration-tests.md).

The built wheel contains only `databricks_es_connector`; neither `tests/` nor `integration_tests/`
is part of the customer artifact.

## License & Attribution

**Copyright © Databricks, Inc.** Developed and maintained by Databricks Forward Deployed Engineering. Available to support customers and the broader community in moving data between Databricks and Elasticsearch. For production support and customization, contact your Databricks account team.

---

**Built with 💜 by Databricks Forward Deployed Engineering**
