# Installation and Setup

How the connector is distributed, what it depends on, and how to install it in a workspace. Do this
once per workspace before running any of the examples in the other docs.

## Distribution

Private Databricks Field Engineering artifact. Not published to PyPI. Distributed to
customers as a built `.whl` (upload to a Unity Catalog Volume, or via Delta Share),
installed in a notebook with `%pip install /Volumes/.../databricks_es_connector-*.whl`.

## Dependencies

The wheel's only direct runtime dependency is `elasticsearch>=8,<9` (declared in
`pyproject.toml`). [`requirements.txt`](../requirements.txt) lists the full resolved
dependency tree with pinned versions (direct and transitive) for security scanning
(SCA / vulnerability review). Spark/pandas are provided by the Databricks runtime and
are not bundled.

## Deploying to a workspace

Upload the wheel to a Unity Catalog Volume (persists across compute; works on serverless):

```bash
databricks fs cp dist/databricks_es_connector-*.whl \
  dbfs:/Volumes/<catalog>/<schema>/<volume>/ --profile <profile>
```

Then install it in a notebook and restart Python:

```python
%pip install /Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl
%restart_python
```

For customer hand-off, share the same `.whl` via a UC Volume in their workspace or
through Delta Share: no code changes, only the `EsWriteConfig` / `EsReadConfig` endpoint/auth differ.

## Elasticsearch credentials

The examples in these docs read the API key from a Databricks secret scope named `es`, key
`api_key` (`dbutils.secrets.get("es", "api_key")`). Create the scope and store the key once per
workspace (the CLI prompts for the value):

```bash
databricks secrets create-scope es
databricks secrets put-secret es api_key
```

Pass the value as `api_key` (or use `basic_auth` for a sandbox) in either config; see
[Connection](configuration.md#connection-shared-by-both-configs).
