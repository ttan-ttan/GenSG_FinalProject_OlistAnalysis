import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create one Spark session for the complete test suite."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-pyspark")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield session

    session.stop()
