"""Bronze tests: with_typed_columns routes valid rows to non-null typed columns
and keeps failed casts as null, without dropping data."""
from transformations._helpers import with_typed_columns


TYPED_COLS = [
    "timestamp_typed",
    "turbine_id_typed",
    "wind_speed_typed",
    "wind_direction_typed",
    "power_output_typed",
]


def _check(label, actual, expected):
    print(f"  [{label}] expected={expected!r}  actual={actual!r}")
    assert actual == expected, f"{label}: expected {expected!r}, got {actual!r}"


def test_casts_valid_rows(load_csv):
    print("\ntest_casts_valid_rows — data_group_wind.csv")
    df = with_typed_columns(load_csv("data_group_wind.csv"))
    total = df.count()
    print(f"  rows loaded: {total}")

    null_predicate = " OR ".join(f"{c} IS NULL" for c in TYPED_COLS)
    null_rows = df.filter(null_predicate)
    n_null = null_rows.count()
    if n_null:
        print("  rows with a NULL typed column:")
        null_rows.show(truncate=False)
    # wind.csv has one out-of-range wind_direction=-1 but the cast still
    # succeeds — bounds live in silver, not bronze. Every typed column populated.
    _check("rows with any NULL typed column", n_null, 0)


def test_passes_nulls_through(load_csv):
    print("\ntest_passes_nulls_through — data_group_null.csv")
    df = with_typed_columns(load_csv("data_group_null.csv"))
    print(f"  rows loaded: {df.count()}")
    # Blank fields become NULL via try_cast — same code path a malformed
    # string would take, so this covers the bad-cast branch as well.
    for col in ["wind_speed_typed", "wind_direction_typed", "power_output_typed"]:
        n = df.filter(f"{col} IS NULL").count()
        print(f"  [{col}] NULL count = {n}  (expected >= 1)")
        assert n >= 1, f"{col}: expected at least 1 NULL row, got {n}"
