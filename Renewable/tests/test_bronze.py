"""Bronze tests: _with_typed_columns routes valid rows to non-null typed columns
and keeps failed casts as null, without dropping data."""
from transformations.bronze import _with_typed_columns


def test_casts_valid_rows(load_csv):
    df = _with_typed_columns(load_csv("data_group_wind.csv"))
    null_rows = df.filter(
        "timestamp_typed IS NULL OR turbine_id_typed IS NULL "
        "OR wind_speed_typed IS NULL OR wind_direction_typed IS NULL "
        "OR power_output_typed IS NULL"
    )
    assert null_rows.count() == 0


def test_passes_nulls_through(load_csv):
    df = _with_typed_columns(load_csv("data_group_null.csv"))
    assert df.filter("wind_speed_typed IS NULL").count() >= 1
    assert df.filter("wind_direction_typed IS NULL").count() >= 1
    assert df.filter("power_output_typed IS NULL").count() >= 1


def test_bad_string_cast_yields_null(spark):
    rows = [("2022-03-01 00:00:00", "1", "abc", "100", "2.5")]
    cols = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
    df = _with_typed_columns(spark.createDataFrame(rows, cols))
    row = df.collect()[0]
    assert row["wind_speed_typed"] is None
    assert row["timestamp_typed"] is not None
    assert row["turbine_id_typed"] == 1
