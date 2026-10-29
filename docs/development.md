# Developing the Connector

For maintainers: working on the library locally, building the wheel, and where the test and release
procedures live.

## Local setup and unit tests

Work on the library locally (on your machine, not Databricks): set up an environment
and run the unit tests:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"      # editable install + test deps
pytest                        # pure-Python tests; no Spark or ES needed
```

To package a release, see [Building the wheel](#building-the-wheel) below.

## Building the wheel

The build packages only `src/databricks_es_connector/` (see
`[tool.hatch.build.targets.wheel]` in `pyproject.toml`); `tests/` is not shipped.

```bash
python -m venv .venv && source .venv/bin/activate   # if not already in one
pip install build                                   # PEP 517 build frontend
python -m build --wheel                             # writes dist/databricks_es_connector-<version>-py3-none-any.whl
```

The version comes from `[project].version` in `pyproject.toml`; bump it there before
building a new release. The wheel lands in `dist/` (git-ignored).

**Cutting a tagged release?** Follow [Releasing](releasing.md): it enumerates the four required
steps (rebuild and upload the wheel, run the tests, run the release gates, tag and attach the wheel).
Four scripts gate a release: `scripts/check_tier_results.py` (the integration tier ran every test),
`scripts/check_requirements_match.py` (requirements.txt matches the wheel's resolved closure),
`scripts/check_readme_sync.py` (every module / fixture / script is documented), and
`scripts/check_version_consistency.py` (every version/wheel reference agrees with `pyproject.toml`).

**When cutting a release, regenerate [`requirements.txt`](../requirements.txt).** The build does
*not* read that file: the wheel declares only the abstract range `elasticsearch>=8,<9`, so a
fresh install resolves transitive versions at install time and drifts from the pinned snapshot.
`requirements.txt` is a point-in-time closure for security scanning; keep it matched to the
release by regenerating it (see the command in its header) whenever dependencies or the pin
change.

Verify the wheel contains only the library (no tests leakage):

```bash
python -c "import zipfile, glob; print('\n'.join(zipfile.ZipFile(sorted(glob.glob('dist/*.whl'))[-1]).namelist()))"
```
