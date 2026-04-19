"""Gold tests: daily summary aggregation math and narrow anomaly projection."""
from pyspark.sql.functions import col, to_date

from transformations.gold import DAILY_SUMMARY_AGGS


COLS = [
    "timestamp", "turbine_id",
    "wind_speed", "wind_direction", "power_output",
    "is_anomaly", "deviation_sigmas",
]


def _summary(df):
    return (
        df.withColumn("date", to_date("timestamp"))
          .groupBy("turbine_id", "date")
          .agg(*DAILY_SUMMARY_AGGS)
    )


def test_daily_summary_math(spark):
    rows = [
        ("2022-03-01 00:00:00", 1, 10.0,  90, 1.0, False, 0.0),
        ("2022-03-01 01:00:00", 1, 12.0, 100, 2.0, False, 0.0),
        ("2022-03-01 02:00:00", 1, 14.0, 110, 3.0, True,  2.5),
        ("2022-03-01 03:00:00", 1, 16.0, 120, 4.0, True,  3.0),
    ]
    df = spark.createDataFrame(rows, COLS)
    out = _summary(df).collect()
    assert len(out) == 1
    r = out[0]
    assert r["min_power_output"]   == 1.0
    assert r["max_power_output"]   == 4.0
    assert r["avg_power_output"]   == 2.5
    assert r["avg_wind_speed"]     == 13.0
    assert r["avg_wind_direction"] == 105.0
    assert r["reading_count"]      == 4
    assert r["anomaly_count"]      == 2


def test_daily_summary_splits_by_date(spark):
    rows = [
        ("2022-03-01 00:00:00", 1, 10.0, 90, 1.0, False, 0.0),
        ("2022-03-01 01:00:00", 1, 12.0, 90, 3.0, False, 0.0),
        ("2022-03-02 00:00:00", 1, 10.0, 90, 5.0, True,  2.5),
        ("2022-03-02 01:00:00", 1, 12.0, 90, 7.0, True,  3.0),
    ]
    df = spark.createDataFrame(rows, COLS)
    out = {str(r["date"]): r for r in _summary(df).collect()}
    assert len(out) == 2
    assert out["2022-03-01"]["avg_power_output"] == 2.0
    assert out["2022-03-01"]["anomaly_count"]    == 0
    assert out["2022-03-02"]["avg_power_output"] == 6.0
    assert out["2022-03-02"]["anomaly_count"]    == 2


def test_anomaly_projection(spark):
    rows = [
        ("2022-03-01 00:00:00", 1, 10.0, 90, 1.0, False, 0.0),
        ("2022-03-01 01:00:00", 1, 10.0, 90, 9.0, True,  3.5),
    ]
    df = spark.createDataFrame(rows, COLS)
    projected = (
        df.filter(col("is_anomaly"))
          .select("turbine_id", "timestamp", "power_output", "deviation_sigmas")
    )
    assert projected.columns == ["turbine_id", "timestamp", "power_output", "deviation_sigmas"]
    assert projected.count() == 1
    assert projected.collect()[0]["power_output"] == 9.0
