"""Shared pytest fixtures. Spark session is provided by Databricks Connect so
transformations run on a remote cluster from a local pytest runner."""
from pathlib import Path

import pytest
from databricks.connect import DatabricksSession


@pytest.fixture(scope="session")
def spark():
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def test_data_dir():
    # tests/ and test/data/ are siblings under Renewable/.
    return Path(__file__).parent.parent / "data"


@pytest.fixture
def load_csv(spark, test_data_dir):
    def _load(name):
        return (
            spark.read
                .option("header", "true")
                .csv(str(test_data_dir / name))
        )
    return _load
