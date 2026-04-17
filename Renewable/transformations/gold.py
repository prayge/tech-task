"""
Gold layer: analytics-ready aggregations for BI and reporting.
"""
from pyspark import pipelines as dp
from pyspark.sql.functions import avg, max as _max, min as _min, count, to_date


@dp.table(
    name="gold.clean",
    comment="Per-turbine, per-day production and wind summary for dashboards."
)
def clean():
    return (
        dp.read("silver.silver_turbine_readings")
            .withColumn("date", to_date("timestamp"))
            .groupBy("turbine_id", "date")
    )