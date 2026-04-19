"""Shared pytest fixtures.

Spark is provided by Databricks Connect. Fixture CSVs live in a Unity Catalog
volume, not on the local filesystem — so the same reads run the same way
from VS Code, from CI, or from a notebook. Upload the files once with:

    databricks fs cp -r test/data /Volumes/colibri/test/data
"""
import pytest
from databricks.connect import DatabricksSession


# Unity Catalog volume holding the test fixtures. Change this in one place only.
FIXTURE_VOLUME = "/Volumes/colibri/test/data"


@pytest.fixture(scope="session")
def spark():
    # Reads config from the environment: DATABRICKS_HOST, DATABRICKS_TOKEN, and
    # either DATABRICKS_CLUSTER_ID (classic) or DATABRICKS_SERVERLESS_COMPUTE_ID
    # (serverless). See run.md.
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture
def load_csv(spark):
    """Return a callable that loads a fixture CSV from the UC volume."""
    def _load(name):
        return (
            spark.read
                .option("header", "true")
                .csv(f"{FIXTURE_VOLUME}/{name}")
        )
    return _load


@pytest.fixture
def bronze_cleansed(spark):
    """Live Delta table — handy for smoke tests against real bronze output."""
    return spark.table("colibri.bronze.bronze_02_cleansed")
