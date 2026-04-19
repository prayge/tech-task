# Wind Turbine Data Pipeline — Colibri

Tech task to Implement, Document, Test a scalable implementation of Databricks

## Overview

Source CSVs (`data_group_{1,2,3}.csv`) land daily in the `01-raw` volume. Auto Loader
ingests incrementally, writing Delta tables at each layer. Runs daily in Triggered mode
via a Databricks Workflow (`TechTask-colibri-daily`) — the single production entry point.
Dev/prod separation via Databricks Asset Bundles (`databricks bundle deploy -t {dev,prod}`).

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

**`silver_01_bounds_invalid`:** Readings that violated one or more bounds, with
a `validation_errors_summary` column naming which ones. Split into its own
table so a Data Engineer can inspect the exceptions in one place rather than
conditionally filtering downstream.

**`silver_02_anomaly_flagged`:** The validated readings with two extra columns
added by a per-turbine, per-day window: `deviation_sigmas` (how many standard
deviations the row's power output sits from its turbine's own daily mean) and
`is_anomaly` (true when the absolute deviation exceeds two standard
deviations). Flagged, not dropped — anomalies are signal. Materialized view,
because the window needs the full daily partition.

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

- **`gold_turbine_daily_summary`:** per-turbine, per-day min/max/avg power
  output, wind stats, `reading_count` (sensor downtime proxy), and
  `anomaly_count`. Reads from `silver_02_anomaly_flagged`.
- **`gold_turbine_anomalies`:** narrow table of flagged readings with deviation
  magnitude, for on-call investigation.

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
in `tests/conftest.py`.

Fixture CSVs live in the Unity Catalog volume `/Volumes/colibri/test/data/`
so the same test code works from VS Code, from CI, or from a notebook
without touching a local filesystem. The repo directory `test/data/` is the
authoritative source for the files; upload it to the volume with a single
CLI call — see `run.md`.

**Coverage (pure helpers, via Databricks Connect):**
- `tests/test_bronze.py` — `_with_typed_columns` routes valid rows to non-null
  typed columns and passes bad-cast rows through as null.
- `tests/test_silver.py` — `dedupe_readings` collapses identical keys, the
  bounds predicates drop out-of-range readings and keep boundary values, and
  `flag_anomalies` marks outliers per turbine per day while respecting
  per-turbine windows and single-row partitions.
- `tests/test_gold.py` — daily aggregation math (min, max, avg, counts,
  anomaly count) and the narrow anomaly projection.

**What pytest does not cover (on purpose):** the `@dp.table`,
`@dp.materialized_view`, and `@dp.expect_all_or_drop` decorators only resolve
inside a running Lakeflow pipeline. Each transformation file therefore
keeps its logic in a plain helper function that the decorated wrapper calls;
tests target the helpers. End-to-end coverage of the decorators, table
linkage, and expectation drops comes from running the pipeline itself via
`databricks bundle run` and inspecting the resulting Delta tables.

See `run.md` for VS Code setup, fixture upload, deploy, and test commands.