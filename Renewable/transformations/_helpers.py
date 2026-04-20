"""Pure Spark DataFrame helpers shared by the DLT pipeline modules.

Kept free of `pyspark.pipelines` imports so they can be exercised by pytest
against CSV fixtures in the Unity Catalog test volume, without needing an
active Databricks declarative-pipeline runtime.
"""
from pyspark.sql.functions import (
    col, lit, coalesce, avg, stddev, to_date,
)
from pyspark.sql.functions import abs as _abs
from pyspark.sql.window import Window


BOUNDS = {
    "valid_wind_direction": "wind_direction BETWEEN 0 AND 360",
    "valid_wind_speed":     "wind_speed >= 0",
    "valid_power_output":   "power_output >= 0",
}


def with_typed_columns(df):
    """Add *_typed columns alongside the original strings. Failed casts become NULL."""
    return (
        df
            .withColumn("timestamp_typed", col("timestamp").cast("timestamp"))
            .withColumn("turbine_id_typed", col("turbine_id").cast("int"))
            .withColumn("wind_speed_typed", col("wind_speed").cast("double"))
            .withColumn("wind_direction_typed", col("wind_direction").cast("int"))
            .withColumn("power_output_typed", col("power_output").cast("double"))
    )


def dedupe_readings(df):
    # Upstream sometimes retry-resends the same reading. Dedup key is
    # (timestamp, turbine_id). Duplicate rows are identical so arbitrary pick is safe.
    return df.dropDuplicates(["timestamp", "turbine_id"])


def flag_anomalies(df):
    # Anomaly: power output more than 2 stddev from that turbine's own daily mean.
    # Single-row partitions produce a null stddev → coalesced to False.
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
