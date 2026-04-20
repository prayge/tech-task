# Running Colibri

Operational guide for developing the pipeline in **VS Code** against
**Databricks compute + Unity Catalog**. Code lives locally; Spark execution,
fixture data, tables, and Lakeflow runs all happen in the workspace. Host
OS: Linux Mint 20.04.

## Prerequisites

- Python 3.11 (`sudo apt install python3.11 python3.11-venv`).
- **VS Code** with the official **Databricks extension** (Marketplace
  publisher `databricks`). It handles auth, cluster/serverless selection,
  "Run on Databricks", and pytest-through-Databricks-Connect without manual
  wiring.
- Databricks CLI v0.229 or newer. The installer writes to `/usr/local/bin`,
  so pipe into `sudo sh`:
  ```
  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
  databricks --version
  ```
- Add in credentials for Host and Token PAT 

## One-time setup

```
git clone <repo> && cd tech-task
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e './Renewable[dev]'
pip install "zstandard>=0.25.0"   # runtime dep of pyspark's Spark Connect client
```

The editable install gives VS Code and pytest working imports of
`transformations.*` while editing source in place. If `from databricks.connect
import DatabricksSession` later raises `PACKAGE_NOT_INSTALLED`, `zstandard`
is the culprit.

## VS Code workflow

1. Install the Databricks extension; `Databricks: Configure workspace` 
2. Open the command palette → `Databricks: Configure cluster` (or select
   serverless). The extension writes `.databricks/` with the choice.
3. Python interpreter: use `./.venv/bin/python` (command palette →
   `Python: Select Interpreter`).
4. Tests tab picks up `pytest` automatically (config lives in
   `Renewable/pyproject.toml`). Click the ▶ next to any test function to run
   it through Databricks Connect against the selected compute.

## Fixture data lives in Unity Catalog

CSVs at the repo root (`data_group_wind.csv`, `data_group_null.csv`,
`data_group_duplicate.csv`) are the source of truth. Upload once to the
workspace volume so the `load_csv` fixture can reach them:

```
databricks fs cp data_group_wind.csv      dbfs:/Volumes/colibri/test/data/
databricks fs cp data_group_null.csv      dbfs:/Volumes/colibri/test/data/
databricks fs cp data_group_duplicate.csv dbfs:/Volumes/colibri/test/data/
databricks fs ls                          dbfs:/Volumes/colibri/test/data
```

Re-upload after editing.

## Tests

Pytest covers the pure helpers in `transformations/_helpers.py`
(`with_typed_columns`, `dedupe_readings`, `flag_anomalies`, `BOUNDS`).
Databricks Connect ships every DataFrame op to serverless — no local JVM.

**Source data:** the repo-root CSVs (`data_group_wind.csv`,
`data_group_null.csv`, `data_group_duplicate.csv`) uploaded to the Unity
Catalog volume `/Volumes/colibri/test/data/` (see "Fixture data" above).
Fixture `load_csv` in `Renewable/tests/conftest.py` reads them from there.

**Run from `tech-task/` (repo root):**

```
.venv/bin/python -m pytest Renewable/tests/ -vv -s --tb=long
```

- `.venv/bin/python -m pytest` — venv's pytest, no shell activation needed.
- `Renewable/tests/` — test dir.
- `-vv` — per-test PASS/FAIL + full assert diffs.
- `-s` — don't capture stdout; the tests `print` expected-vs-actual counts.
- `-tb=long` — full traceback on failure.

**Why helpers only:** `@dp.table` / `@dp.materialized_view` (in `bronze.py`,
`silver.py`) register into an active Lakeflow graph at import time, so
importing those modules outside a pipeline run raises
`GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE`. End-to-end coverage
of the decorated wrappers comes from `databricks bundle run` below.

## Deploy

`databricks.yml` has no hardcoded host — relies on `DATABRICKS_HOST` from the
shell. Then:

```
cd Renewable
databricks bundle validate -t dev     # sanity-check databricks.yml
databricks bundle deploy  -t dev      # creates pipeline + job in the workspace
databricks bundle deploy  -t prod     # same, to the prod target
```

## Run the pipeline

Single entry point:

```
databricks bundle run job_colibri_daily -t dev
```

`job_colibri_daily` is a Databricks job that triggers the
`pipeline_colibri_pipeline` Lakeflow pipeline. The pipeline discovers every
file under `transformations/` via the bundle glob and runs bronze → silver →
gold in dependency order automatically.

## Observing counts between stages

Every transformation module emits through `logging.getLogger(__name__)`;
Lakeflow routes those records to the pipeline event log. Lakeflow also emits
a built-in `flow_progress` event per table on every update with input and
output row counts, so the between-stage numbers are tracked without any
custom counter code:

- Bronze type-cast failures → rows written to `colibri.<target>.bronze_02_invalid`.
- Silver bounds drops → rows in `silver_01_invalid`, plus the expectation
  metrics on `silver_01_bounds_validated`.
- Silver duplicates collapsed → input vs output row count on
  `silver_01_bounds_validated` in `flow_progress`.
- Anomalies flagged → `is_anomaly = true` rows on `silver_01_bounds_validated`
  (anomaly flagging now lives on the validated table).
- Per-turbine daily aggregates → `gold_turbine_daily_summary`.

Inspect from the CLI:

```
databricks pipelines list-pipeline-events --pipeline-id <id>
```

Or query `event_log("<pipeline-id>")` from a notebook / SQL editor.

## Exporting `requirements.txt` from `pyproject.toml`

For CI images or `pip install -r` flows:

```
pip install pip-tools
pip-compile Renewable/pyproject.toml --extra dev -o requirements.txt
```

Re-run after editing `pyproject.toml`. Or the quick-and-dirty route after a
local install: `pip freeze --exclude-editable > requirements.txt` (leaks
whatever else is in the venv — less hygienic).
