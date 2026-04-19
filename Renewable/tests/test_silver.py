"""Silver tests: dedup collapses identical keys, bounds predicates drop
out-of-range readings, anomaly flagger marks readings more than two standard
deviations from the per-turbine, per-day mean."""
from transformations.bronze import _with_typed_columns
from transformations.silver import BOUNDS, dedupe_readings, flag_anomalies


def _typed(load_csv, name):
    """Load a fixture CSV and project the cast columns back to their real names."""
    df = _with_typed_columns(load_csv(name))
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
    assert dedupe_readings(df).count() == 4   # 4 unique (timestamp, turbine_id) pairs


def test_bounds_drop_out_of_range(load_csv):
    df = _typed(load_csv, "data_group_wind.csv")
    assert df.count() == 50
    kept = df.filter(" AND ".join(BOUNDS.values()))
    assert kept.count() == 49
    assert kept.filter("wind_direction = -1").count() == 0


def test_bounds_keep_edge_values(spark):
    rows = [
        ("2022-03-01 00:00:00", 1, 0.0,   0, 0.0),
        ("2022-03-01 00:00:00", 2, 0.0, 360, 0.0),
        ("2022-03-01 00:00:00", 3, 5.0, 180, 1.0),
    ]
    cols = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
    df = spark.createDataFrame(rows, cols)
    kept = df.filter(" AND ".join(BOUNDS.values()))
    assert kept.count() == 3


def test_flag_anomalies_marks_outlier(spark):
    rows = [("2022-03-01 00:00:00", 1, 10.0, 100, p) for p in [2.0] * 9 + [20.0]]
    cols = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
    df = spark.createDataFrame(rows, cols)
    out = flag_anomalies(df).collect()
    flagged = [r for r in out if r["is_anomaly"]]
    assert len(flagged) == 1
    assert flagged[0]["power_output"] == 20.0
    assert abs(flagged[0]["deviation_sigmas"]) > 2


def test_flag_anomalies_partitions_by_turbine(spark):
    # Turbine 1: tight spread around 2.0. Turbine 2: wide spread around 10.0.
    # The 14.0 reading on turbine 2 is within turbine 2's own spread.
    rows = (
        [("2022-03-01 00:00:00", 1, 10.0, 100, p) for p in [2.0, 2.0, 2.0, 2.1, 1.9]]
        + [("2022-03-01 00:00:00", 2, 10.0, 100, p) for p in [6.0, 8.0, 10.0, 12.0, 14.0]]
    )
    cols = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
    df = spark.createDataFrame(rows, cols)
    out = {(r["turbine_id"], r["power_output"]): r["is_anomaly"] for r in flag_anomalies(df).collect()}
    assert out[(2, 14.0)] is False            # within turbine 2's own window


def test_flag_anomalies_single_row_not_flagged(spark):
    rows = [("2022-03-01 00:00:00", 1, 10.0, 100, 2.0)]
    cols = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
    df = spark.createDataFrame(rows, cols)
    out = flag_anomalies(df).collect()[0]
    assert out["is_anomaly"] is False         # stddev is null for single-row partition
