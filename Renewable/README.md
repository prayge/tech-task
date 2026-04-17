# Wind Turbine Data Pipeline — Colibri

Tech task to Implement, Document, Test a scalable implementation of Databricks.

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

Business rules and anomaly flagging on cleansed readings.


**`silver_01_validated`:** Rows where every column cast cleanly to its target type.
Feeds silver.

**`silver_01_invalid`:** Rows where at least one cast failed, with a
`validation_errors_summary` column naming which columns failed. Split into its own
table so exceptions are inspectable in one place, cleaner than conditional filtering
downstream. All rows in here have a column for the edge case in which they are invalid for, determining easy solution and scanning
- **Anomaly flagging:** readings more than 2 standard deviations from the turbine's own mean are
  flagged via an `is_anomaly` column. Flagged, not dropped — anomalies are signal.


**`silver_02_transformed`:** Rows where every column cast cleanly to its target type.
Feeds silver.
  - **Dedup** on `(timestamp, turbine_id)` — upstream occasionally double-sends on retry.
  - **Bounds checks** via `@dp.expect_or_drop`: `wind_direction BETWEEN 0 AND 360`,
    `wind_speed BETWEEN 0 AND 150`, `power_output >= 0`. Readings outside physical
    envelopes are sensor malfunctions, not data.


**Assumptions:** Anomaly detection is per-turbine (each unit has its own baseline)
over single day

## Gold

Analytics-ready daily summaries. Materialized views refresh on each pipeline run.

- **`gold_turbine_daily_summary`:** per-turbine, per-day min/max/avg power output,
  wind stats, `reading_count` (sensor downtime proxy), and `anomaly_count`.
- **`gold_turbine_anomalies`:** narrow table of flagged readings with deviation
  magnitude, for on-call investigation.

**Assumptions:** Daily aggregation satisfies the "24 hours" requirement from the
brief; moving to hourly is a schedule change, not a code change.

## Testing

Configured against Databricks Connect — transformations run on a remote cluster fromhttps://github.com/prayge/tech-task.githttps://github.com/prayge/tech-task.git
a local `pytest` runner. Auth via `~/.databrickscfg` or `DATABRICKS_*` env vars in CI.
Session-scoped `spark` fixture in `tests/conftest.py`.

Run with `pytest -v`. Each test builds an in-memory DataFrame, pushes it through the
transformation, asserts on the output.

**Intended coverage (scaffolded):** bronze cast routing, silver dedup, silver bounds,
silver anomaly detection, gold aggregation correctness.

**Assumptions:** Transformation helpers would move to `transformations/_helpers.py` so
they can be imported from both pipeline files and tests — pipeline files execute inside
the Databricks runtime and aren't cleanly importable by pytest.