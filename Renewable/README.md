# Wind Turbine Data Pipeline — Colibri

Tech task to Implement, Document, Test a scalable implementation of Databricks

## Overview

Source CSVs (`data_group_{1,2,3}.csv`) land daily in the `01-raw` volume. Auto Loader
ingests incrementally, writing Delta tables at each layer. Runs daily in Triggered mode
via a Databricks Workflow (`TechTask-colibri-daily`) — the single production entry point.
Dev/prod separation via Databricks Asset Bundles (`databricks bundle deploy -t {dev,prod}`).
Bundle config in `databricks.yml`: serverless pipeline `colibri_pipeline_${bundle.target}`,
catalog `colibri`, schema `${bundle.target}`, libraries globbed from `transformations/**`.

## Bronze

Two-stage ingestion and validation.

**`bronze_01_raw`:** Auto Loader reads CSVs as strings. No schema enforcement.
Immutable audit trail of everything that arrived.

**`bronze_02_cleansed`:** Rows where every column cast cleanly to its target type.
Feeds silver.

**`bronze_02_invalid`:** Rows where at least one cast failed, with a
`validation_errors_summary` column naming which columns failed. Split into its own
table so exceptions are inspectable in one place, cleaner than conditional filtering
downstream.

**Assumptions:**
- CSV schema is stable; `schemaLocation` enforces the inferred schema
  after the first run.
- A Data Engineer would be paying attention to invalid rows, potentially email alerts on a clientside implementation.

## Silver

Bounds validation, dedup, and anomaly flagging on cleansed readings.

**`silver_01_bounds_validated`:** Readings that pass the physical bounds
predicates `wind_direction BETWEEN 0 AND 360`, `wind_speed >= 0`,
`power_output >= 0`, then deduped on `(timestamp, turbine_id)`. Bounds are
enforced with `@dp.expect_all_or_drop`; dedup handles upstream retry resends.

**`silver_01_invalid`:** Readings that violated one or more bounds or duplicate
the `(timestamp, turbine_id)` key, with a `validation_errors_summary` column
naming which ones. Split into its own table so a Data Engineer can inspect the
exceptions in one place rather than conditionally filtering downstream.

**`silver_02_anomaly_flagged`:** Anomalous readings only — rows whose
`power_output` lies more than two standard deviations from that turbine's own
daily mean. Each row carries `deviation_sigmas` (signed σ distance), plus
`observations` (count of validated readings for that turbine on that day) and
`mean_power_output` (the daily mean used as the anomaly baseline) so the flag
can be verified at a glance without re-joining silver. Materialized view —
the σ window needs the full daily partition, which streaming cannot see.

**Assumptions:**
- The anomaly baseline is per turbine per day. Each unit has its own profile,
  and a single day is the window the brief calls out.
- A single-reading partition has no standard deviation to compare against, so
  that reading is treated as not anomalous rather than being flagged on
  missing information.
- Duplicate rows on the same `(timestamp, turbine_id)` are identical payloads
  from the retry path, so an arbitrary pick during dedup is safe.

## Gold

Analytics-ready daily summaries. Materialized views refresh on each pipeline run.

- **`gold_turbine_daily_summary`:** per-turbine, per-day min/max/avg
  `power_output`, wind stats, and `reading_count` (sensor downtime proxy).
  Reads from `silver_01_bounds_validated` so the aggregations cover every
  validated reading, not just the anomalous ones. Anomaly investigation
  lives in `silver_02_anomaly_flagged`, joinable on `(turbine_id, date)`.

**Assumptions:**
- A daily aggregation satisfies the "over a given time period, for example 24
  hours" requirement from the brief. Moving to hourly would be a schedule
  change, not a code change.
- The `reading_count` per turbine per day is a usable proxy for sensor
  downtime. The scenario explicitly notes that sensors can miss entries.

## Testing

Configured against Databricks Connect — transformations run on Databricks
compute (serverless by default) from a `pytest` runner in VS Code. Auth via
`DATABRICKS_*` env vars sourced from `.env`. Session-scoped `spark` fixture
in `tests/conftest.py`. `pyproject.toml` pins `databricks-connect==15.1.*` to
match the target cluster's DBR; bump that line when the cluster is upgraded.

Fixture CSVs live in the Unity Catalog volume `/Volumes/colibri/test/data/`
so the same test code works from VS Code, from CI, or from a notebook
without touching a local filesystem. The repo directory `test/data/` is the
authoritative source for the files; upload it to the volume with a single
CLI call — see `run.md`. Every test reads from one of three fixtures
(`data_group_wind.csv`, `data_group_null.csv`, `data_group_duplicate.csv`);
no test builds inline DataFrames, so behaviour stays anchored to real data
shapes.

**Coverage (pure helpers, via Databricks Connect):**
- `tests/test_bronze.py` — `_with_typed_columns` populates every typed column
  on clean rows (`data_group_wind.csv`) and passes blank/bad values through as
  null (`data_group_null.csv`).
- `tests/test_silver.py` — `dedupe_readings` collapses 22 duplicate rows down
  to 4 unique keys (`data_group_duplicate.csv`), the `BOUNDS` predicates drop
  the one out-of-range reading from `data_group_wind.csv`, and
  `flag_anomalies` adds `is_anomaly` + `deviation_sigmas` without dropping
  rows or producing nulls — the null-safety invariant for single-row
  partitions.

**Running inside Databricks:** the `colibri-tests` job in `databricks.yml`
invokes the `tests/run_tests` notebook, which shells out to `pytest.main()`
against the bundle-uploaded `tests/` directory. Adding new `test_*.py` files
needs no job wiring — pytest discovers them. Run with:

    databricks bundle run colibri-tests -t dev

A failing assertion fails the cell, which fails the task, which fails the job.

**What pytest does not cover (on purpose):** the `@dp.table`,
`@dp.materialized_view`, and `@dp.expect_all_or_drop` decorators only resolve
inside a running Lakeflow pipeline. Each transformation file therefore
keeps its logic in a plain helper function that the decorated wrapper calls;
tests target the helpers. End-to-end coverage of the decorators, table
linkage, and expectation drops comes from running the pipeline itself via
`databricks bundle run` and inspecting the resulting Delta tables.

