import pytest
from pyspark.sql import SparkSession

from src.validation_orders import validate_orders


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for validation tests."""

    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_validation_orders")
        .getOrCreate()
    )

    yield spark_session
    spark_session.stop()


def create_orders_df(spark, data):
    """Create a test Orders dataframe."""

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

    return spark.createDataFrame(data, columns)


def test_valid_orders(spark):
    """Valid Orders data should pass validation."""

    data = [
        (
            "order_001",
            "customer_001",
            "delivered",
            "2018-01-10 10:00:00",
            "2018-01-10 11:00:00",
            "2018-01-12 09:00:00",
            "2018-01-15 14:00:00",
            "2018-01-20 00:00:00",
        )
    ]

    df = create_orders_df(spark, data)

    result = validate_orders(df)

    assert result.count() == 1


def test_duplicate_order_id(spark):
    """Duplicate order_id should fail validation."""

    data = [
        (
            "order_001",
            "customer_001",
            "delivered",
            "2018-01-10 10:00:00",
            "2018-01-10 11:00:00",
            "2018-01-12 09:00:00",
            "2018-01-15 14:00:00",
            "2018-01-20 00:00:00",
        ),
        (
            "order_001",
            "customer_002",
            "shipped",
            "2018-01-11 10:00:00",
            "2018-01-11 11:00:00",
            "2018-01-12 09:00:00",
            "2018-01-15 14:00:00",
            "2018-01-21 00:00:00",
        ),
    ]

    df = create_orders_df(spark, data)

    with pytest.raises(
        ValueError,
        match="Duplicate order_id values detected"
    ):
        validate_orders(df)


def test_invalid_order_status(spark):
    """Invalid order_status should fail validation."""

    data = [
        (
            "order_001",
            "customer_001",
            "banana",
            "2018-01-10 10:00:00",
            "2018-01-10 11:00:00",
            "2018-01-12 09:00:00",
            "2018-01-15 14:00:00",
            "2018-01-20 00:00:00",
        )
    ]

    df = create_orders_df(spark, data)

    with pytest.raises(
        ValueError,
        match="Invalid order_status detected"
    ):
        validate_orders(df)