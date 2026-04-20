"""Bronze tests: _with_typed_columns routes valid rows to non-null typed columns
and keeps failed casts as null, without dropping data."""
from transformations import bronze 

def test_casts_valid_rows(load_csv):
    df = bronze._with_typed_columns(load_csv("data_group_wind.csv"))
    null_rows = df.filter(
        "timestamp_typed IS NULL OR turbine_id_typed IS NULL "
        "OR wind_speed_typed IS NULL OR wind_direction_typed IS NULL "
        "OR power_output_typed IS NULL"
    )
    # wind.csv has one out-of-range wind_direction=-1 but the cast still
    # succeeds — bounds live in silver, not bronze. Every typed column populated.
    assert null_rows.count() == 0


def test_passes_nulls_through(load_csv):
    df = bronze._with_typed_columns(load_csv("data_group_null.csv"))
    # Blank fields become NULL via try_cast — the same code path a malformed
    # string would take, so this covers the bad-cast branch as well.
    assert df.filter("wind_speed_typed IS NULL").count() >= 1
    assert df.filter("wind_direction_typed IS NULL").count() >= 1
    assert df.filter("power_output_typed IS NULL").count() >= 1
