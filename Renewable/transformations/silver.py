import logging

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, when, lit, array, array_compact, concat_ws, count,
)
from pyspark.sql.window import Window

from transformations._helpers import BOUNDS, dedupe_readings, flag_anomalies


logger = logging.getLogger(__name__)


@dp.materialized_view(
    name="silver_01_bounds_validated",
    comment="Bounds-checked, deduped readings with anomaly flag. Feeds gold.",
)
@dp.expect_all_or_drop(BOUNDS)
def silver_01_bounds_validated():
    # Materialized (not streaming) because flag_anomalies needs the full
    # daily window per turbine. 
    logger.info("silver_01_bounds_validated: bounds + dedup + anomaly flag on bronze_02_cleansed")
    return (
        flag_anomalies(dedupe_readings(dp.read("bronze_02_cleansed")))
    )


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


