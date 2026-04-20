"""Shared pytest fixtures. Builds a Spark session via Databricks Connect
when running locally, or reuses the job's SparkSession when running inside
a Databricks task. Exposes a load_csv fixture that reads CSVs from the
Unity Catalog volume /Volumes/colibri/test/data/."""
import os
import sys
from pathlib import Path

# Make `transformations` importable when pytest is launched from a parent
# directory (e.g. VS Code's workspace root points at tech-task/, not
# Renewable/). Prepending Renewable/ here runs before test collection.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest


TEST_DATA_VOLUME = "/Volumes/colibri/test/data"


@pytest.fixture(scope="session")
def spark():
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.appName("colibri-tests").getOrCreate()


@pytest.fixture
def load_csv(spark):
    def _load(name: str):
        path = os.path.join(TEST_DATA_VOLUME, name)
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(path)
        )
    return _load
