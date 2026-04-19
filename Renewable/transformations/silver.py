import logging

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, when, lit, array, array_compact, concat_ws,
    avg, stddev, coalesce, count, to_date,
)
from pyspark.sql.functions import abs as _abs
from pyspark.sql.window import Window


logger = logging.getLogger(__name__)

# Physical bounds for a valid sensor reading. Values outside these envelopes are
# sensor malfunctions, not data. Kept as a dict so the silver expectation and
# the invalid-table reason logic share the same predicates.
BOUNDS = {
    "valid_wind_direction": "wind_direction BETWEEN 0 AND 360",
    "valid_wind_speed":     "wind_speed >= 0",
    "valid_power_output":   "power_output >= 0",
}


def dedupe_readings(df):
    # Upstream sometimes retry-resends the same reading. Dedup key is
    # (timestamp, turbine_id). Duplicate rows are identical so arbitrary pick is safe.
    return df.dropDuplicates(["timestamp", "turbine_id"])


def flag_anomalies(df):
    # Anomaly definition: a reading whose power output is more than two standard
    # deviations away from that turbine's own mean for the same day. Window is
    # per turbine, per day. Single-row partitions produce a null standard
    # deviation and are treated as not anomalous.
    w = Window.partitionBy("turbine_id", to_date("timestamp"))
    return (
        df
            .withColumn("_mean",  avg("power_output").over(w))
            .withColumn("_sigma", stddev("power_output").over(w))
            .withColumn(
                "deviation_sigmas",
                (col("power_output") - col("_mean")) / col("_sigma"),
            )
            .withColumn(
                "is_anomaly",
                coalesce(_abs(col("deviation_sigmas")) > lit(2.0), lit(False)),
            )
            .drop("_mean", "_sigma")
    )


@dp.table(
    name="silver_01_bounds_validated",
    comment="Bounds-checked, deduped readings. Feeds anomaly flagging and gold.",
)
@dp.expect_all_or_drop(BOUNDS)
def silver_01_bounds_validated():
    logger.info("silver_01_bounds_validated: bounds + dedup on bronze_02_cleansed")
    return dedupe_readings(dp.read_stream("bronze_02_cleansed"))


@dp.materialized_view(
    name="silver_01_invalid",
    comment="Readings rejected from validated: out-of-bounds or duplicate (timestamp, turbine_id).",
)
def silver_01_invalid():
    # Materialized, not streaming: duplicate detection needs a window count over
    # the full partition. Reads bronze_02_cleansed (already type-validated).
    logger.info("silver_01_invalid: capturing out-of-bounds and duplicate readings")
    dup_window = Window.partitionBy("timestamp", "turbine_id")
    return (
        dp.read("bronze_02_cleansed")
            .withColumn("_dup_count", count(lit(1)).over(dup_window))
            .withColumn(
                "validation_errors",
                array_compact(array(
                    when(~col("wind_direction").between(0, 360), lit("wind_direction")),
                    when(col("wind_speed")   < 0, lit("wind_speed")),
                    when(col("power_output") < 0, lit("power_output")),
                    when(col("_dup_count") > 1, lit("duplicate")),
                )),
            )
            .filter("size(validation_errors) > 0")
            .withColumn(
                "validation_errors_summary",
                concat_ws(",", col("validation_errors")),
            )
            .drop("validation_errors", "_dup_count")
    )


@dp.materialized_view(
    name="silver_02_anomaly_flagged",
    comment="Anomalous readings only (>2σ from per-turbine, per-day mean), with daily mean and observation count for verification.",
)
def silver_02_anomaly_flagged():
    # Materialized, not streaming: the window needs the full daily partition.
    logger.info("silver_02_anomaly_flagged: emitting anomalies per turbine per day")
    w = Window.partitionBy("turbine_id", to_date("timestamp"))
    return (
        flag_anomalies(dp.read("silver_01_bounds_validated"))
            .withColumn("observations", count(lit(1)).over(w))
            .withColumn("mean_power_output", avg("power_output").over(w))
            .filter(col("is_anomaly"))
            .drop("is_anomaly")
    )
