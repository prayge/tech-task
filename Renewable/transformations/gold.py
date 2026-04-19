"""Gold layer: analytics-ready aggregations for BI and reporting."""
import logging

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, lit, avg, count, to_date,
)
from pyspark.sql.functions import (
    min as _min, max as _max, sum as _sum,
)


logger = logging.getLogger(__name__)

# Shared agg spec. Imported by test_gold.py so tests and production stay in lockstep.
# sum(is_anomaly) needs the int cast because Spark SQL does not sum booleans.
DAILY_SUMMARY_AGGS = [
    _min("power_output").alias("min_power_output"),
    _max("power_output").alias("max_power_output"),
    avg("power_output").alias("avg_power_output"),
    avg("wind_speed").alias("avg_wind_speed"),
    avg("wind_direction").alias("avg_wind_direction"),
    count(lit(1)).alias("reading_count"),
    _sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
]


@dp.materialized_view(
    name="gold_turbine_daily_summary",
    comment="Per-turbine, per-day min/max/avg power output, wind stats, reading count, anomaly count.",
)
def gold_turbine_daily_summary():
    logger.info("gold_turbine_daily_summary: aggregating silver_02_anomaly_flagged")
    return (
        dp.read("silver_02_anomaly_flagged")
            .withColumn("date", to_date("timestamp"))
            .groupBy("turbine_id", "date")
            .agg(*DAILY_SUMMARY_AGGS)
    )


@dp.materialized_view(
    name="gold_turbine_anomalies",
    comment="Narrow table of anomalous readings for on-call investigation.",
)
def gold_turbine_anomalies():
    logger.info("gold_turbine_anomalies: projecting flagged rows")
    return (
        dp.read("silver_02_anomaly_flagged")
            .filter(col("is_anomaly"))
            .select("turbine_id", "timestamp", "power_output", "deviation_sigmas")
    )
