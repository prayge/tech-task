import logging

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, when, lit, array, array_compact, concat_ws,
    current_timestamp, input_file_name,
)


logger = logging.getLogger(__name__)

RAW_PATH = "/Volumes/colibri/bronze/01-raw/"
SCHEMA_LOCATION = "/Volumes/colibri/bronze/01-raw/_schemas/bronze_01/_schemas/"


# ---------- helper ----------

def _with_typed_columns(df):
    """Add *_typed columns alongside the original strings. Failed casts become NULL."""
    return (
        df
            .withColumn("timestamp_typed", col("timestamp").cast("timestamp"))
            .withColumn("turbine_id_typed", col("turbine_id").cast("int"))
            .withColumn("wind_speed_typed", col("wind_speed").cast("double"))
            .withColumn("wind_direction_typed", col("wind_direction").cast("int"))
            .withColumn("power_output_typed", col("power_output").cast("double"))
    )
# ---------- Stage 1: raw ingestion, everything as strings ----------

@dp.table(
    name="bronze_01_raw",
    comment="Raw turbine CSVs ingested as strings. No validation. Immutable landing."
)
def bronze_01_raw():
    logger.info("bronze_01_raw: Auto Loader reading %s", RAW_PATH)
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
            .option("cloudFiles.inferColumnTypes", "false")
            .load(RAW_PATH)
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
    )


# ---------- Stage 2: typed validation, split into cleansed and invalid ----------

@dp.table(
    name="bronze_02_cleansed",
    comment="Rows from bronze_01_raw that cast cleanly to the expected schema."
)
@dp.expect_all_or_drop({
    "valid_timestamp": "timestamp IS NOT NULL",
    "valid_turbine_id": "turbine_id IS NOT NULL",
    "valid_wind_speed": "wind_speed IS NOT NULL",
    "valid_wind_direction": "wind_direction IS NOT NULL",
    "valid_power_output": "power_output IS NOT NULL",
})
def bronze_02_cleansed():
    return _with_typed_columns(dp.read_stream("bronze_01_raw")).select(
        col("timestamp_typed").alias("timestamp"),
        col("turbine_id_typed").alias("turbine_id"),
        col("wind_speed_typed").alias("wind_speed"),
        col("wind_direction_typed").alias("wind_direction"),
        col("power_output_typed").alias("power_output"),
        col("_ingested_at"),
        col("_source_file"),
    )


@dp.table(
    name="bronze_02_invalid",
    comment="Rows where at least one column failed type casting, with reasons."
)
def bronze_02_invalid():
    typed = _with_typed_columns(dp.read_stream("bronze_01_raw"))

    return (
        typed
            .withColumn(
                "validation_errors",
                array_compact(array(
                    when(col("timestamp_typed").isNull() & col("timestamp").isNotNull(), lit("timestamp")),
                    when(col("turbine_id_typed").isNull() & col("turbine_id").isNotNull(), lit("turbine_id")),
                    when(col("wind_speed_typed").isNull() & col("wind_speed").isNotNull(), lit("wind_speed")),
                    when(col("wind_direction_typed").isNull() & col("wind_direction").isNotNull(), lit("wind_direction")),
                    when(col("power_output_typed").isNull() & col("power_output").isNotNull(), lit("power_output")),
                ))
            )
            .filter("size(validation_errors) > 0")
            .select(
                col("timestamp"),
                col("turbine_id"),
                col("wind_speed"),
                col("wind_direction"),
                col("power_output"),
                concat_ws(",", col("validation_errors")).alias("validation_errors_summary"),
                col("_ingested_at"),
                col("_source_file"),
            )
    )

