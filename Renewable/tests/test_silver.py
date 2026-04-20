"""Silver tests: dedup collapses identical keys, bounds predicates drop
out-of-range readings, anomaly flagger adds a null-safe is_anomaly column."""
from transformations._helpers import (
    BOUNDS, dedupe_readings, flag_anomalies, with_typed_columns,
)


def _check(label, actual, expected):
    """Print EXPECTED / ACTUAL before asserting so passing runs still log evidence."""
    print(f"  [{label}] expected={expected!r}  actual={actual!r}")
    assert actual == expected, f"{label}: expected {expected!r}, got {actual!r}"


def _typed(load_csv, name):
    """Load a fixture CSV and project the cast columns back to their real names."""
    df = with_typed_columns(load_csv(name))
    return df.selectExpr(
        "timestamp_typed AS timestamp",
        "turbine_id_typed AS turbine_id",
        "wind_speed_typed AS wind_speed",
        "wind_direction_typed AS wind_direction",
        "power_output_typed AS power_output",
    )


def test_dedupe_readings_collapses_duplicates(load_csv):
    print("\ntest_dedupe_readings_collapses_duplicates — data_group_duplicate.csv")
    df = _typed(load_csv, "data_group_duplicate.csv")
    _check("total rows loaded", df.count(), 22)
    _check("rows after dedupe", dedupe_readings(df).count(), 4)


def test_bounds_drop_out_of_range(load_csv):
    print("\ntest_bounds_drop_out_of_range — data_group_wind.csv")
    df = _typed(load_csv, "data_group_wind.csv")
    _check("total rows loaded", df.count(), 50)

    kept = df.filter(" AND ".join(BOUNDS.values()))
    dropped = df.subtract(kept).select("wind_direction", "wind_speed", "power_output")
    print("  dropped rows (should be wind_direction=-1 and 366):")
    for r in dropped.collect():
        print(f"    {r.asDict()}")

    _check("rows kept after bounds filter", kept.count(), 48)
    _check(
        "rows in kept with out-of-range wind_direction",
        kept.filter("wind_direction < 0 OR wind_direction > 360").count(),
        0,
    )


def test_flag_anomalies_shape_and_null_safety(load_csv):
    print("\ntest_flag_anomalies_shape_and_null_safety — data_group_wind.csv")
    df = _typed(load_csv, "data_group_wind.csv")
    out = flag_anomalies(df)

    in_rows, out_rows = df.count(), out.count()
    _check("row count preserved by flag_anomalies", out_rows, in_rows)

    added = set(out.columns) - set(df.columns)
    print(f"  columns added by flag_anomalies: {sorted(added)}")
    assert {"is_anomaly", "deviation_sigmas"} <= added, (
        f"expected is_anomaly + deviation_sigmas in added cols, got {sorted(added)}"
    )

    null_anoms = out.filter("is_anomaly IS NULL").count()
    anoms_true = out.filter("is_anomaly = true").count()
    print(f"  anomalies flagged true: {anoms_true}  /  null is_anomaly: {null_anoms}")
    _check("null is_anomaly rows (coalesce must zero these)", null_anoms, 0)
