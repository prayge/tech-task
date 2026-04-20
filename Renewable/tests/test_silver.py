"""Silver tests: dedup collapses identical keys, bounds predicates drop
out-of-range readings, anomaly flagger adds a null-safe is_anomaly column."""
from transformations import bronze, silver


def _typed(load_csv, name):
    """Load a fixture CSV and project the cast columns back to their real names."""
    df = bronze._with_typed_columns(load_csv(name))
    return df.selectExpr(
        "timestamp_typed AS timestamp",
        "turbine_id_typed AS turbine_id",
        "wind_speed_typed AS wind_speed",
        "wind_direction_typed AS wind_direction",
        "power_output_typed AS power_output",
    )


def test_dedupe_readings_collapses_duplicates(load_csv):
    df = _typed(load_csv, "data_group_duplicate.csv")
    assert df.count() == 22                   # sanity: fixture has 22 data rows
    assert silver.dedupe_readings(df).count() == 4   # 4 unique (timestamp, turbine_id) pairs


def test_bounds_drop_out_of_range(load_csv):
    df = _typed(load_csv, "data_group_wind.csv")
    assert df.count() == 50
    kept = df.filter(" AND ".join(silver.BOUNDS.values()))
    assert kept.count() == 49
    assert kept.filter("wind_direction = -1").count() == 0


def test_flag_anomalies_shape_and_null_safety(load_csv):
    df = _typed(load_csv, "data_group_wind.csv")
    out = silver.flag_anomalies(df)
    # Pure column-add: no rows dropped.
    assert out.count() == df.count()
    # Shape contract held by downstream gold.
    assert "is_anomaly" in out.columns
    assert "deviation_sigmas" in out.columns
    # coalesce(..., False) guarantees no nulls even if stddev is null for a
    # single-row per-turbine-per-day partition.
    assert out.filter("is_anomaly IS NULL").count() == 0
