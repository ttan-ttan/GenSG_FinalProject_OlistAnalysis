from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.validation_orders import validate_orders


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


def make_valid_orders(spark):
    columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    row = (
        "order-1",
        "customer-1",
        "delivered",
        datetime(2018, 1, 1, 10),
        datetime(2018, 1, 1, 11),
        datetime(2018, 1, 2, 10),
        datetime(2018, 1, 3, 10),
        datetime(2018, 1, 4, 10),
    )
    return spark.createDataFrame([row], columns)


def test_validate_orders_rejects_carrier_date_before_purchase(spark):
    df = make_valid_orders(spark).withColumn(
        "order_delivered_carrier_date",
        F.lit(datetime(2017, 12, 31, 10)),
    )

    with pytest.raises(ValueError, match="order_delivered_carrier_date"):
        validate_orders(df)


def test_validate_orders_allows_missing_carrier_date(spark):
    df = make_valid_orders(spark).withColumn(
        "order_delivered_carrier_date",
        F.lit(None).cast("timestamp"),
    )

    assert validate_orders(df).count() == 1


@pytest.mark.parametrize(
    ("column", "data_type", "error"),
    [
        ("order_status", "string", "order_status"),
        (
            "order_purchase_timestamp",
            "timestamp",
            "order_purchase_timestamp",
        ),
    ],
)
def test_validate_orders_rejects_null_lifecycle_fields(
    spark, column, data_type, error
):
    df = make_valid_orders(spark).withColumn(
        column, F.lit(None).cast(data_type)
    )

    with pytest.raises(ValueError, match=error):
        validate_orders(df)
