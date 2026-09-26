import pytest

from src.validation_order_items import validate_order_items


def test_validate_order_items_valid(spark):
    """Ensure valid order item records pass validation."""

    df = spark.createDataFrame(
        [
            ("O001", 1, "P001", "S001", "2017-10-10 15:00:00", 99.90, 12.50),
            ("O001", 2, "P002", "S002", "2017-10-11 15:00:00", 50.00, 0.00),
        ],
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    )

    df = df.withColumn(
        "shipping_limit_date", df["shipping_limit_date"].cast("timestamp")
    )

    validated = validate_order_items(df)

    assert validated.count() == 2


def test_validate_order_items_duplicate_combination(spark):
    """Ensure duplicate order_id + order_item_id fails validation."""

    df = spark.createDataFrame(
        [
            ("O001", 1, "P001", "S001", "2017-10-10 15:00:00", 99.90, 12.50),
            ("O001", 1, "P002", "S002", "2017-10-11 15:00:00", 50.00, 5.00),
        ],
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    )

    df = df.withColumn(
        "shipping_limit_date", df["shipping_limit_date"].cast("timestamp")
    )

    with pytest.raises(ValueError):
        validate_order_items(df)


def test_validate_order_items_invalid_price(spark):
    """Ensure zero or negative price fails validation."""

    df = spark.createDataFrame(
        [
            ("O001", 1, "P001", "S001", "2017-10-10 15:00:00", 0.00, 12.50),
        ],
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    )

    df = df.withColumn(
        "shipping_limit_date", df["shipping_limit_date"].cast("timestamp")
    )

    with pytest.raises(ValueError):
        validate_order_items(df)


def test_validate_order_items_negative_freight(spark):
    """Ensure negative freight value fails validation."""

    df = spark.createDataFrame(
        [
            ("O001", 1, "P001", "S001", "2017-10-10 15:00:00", 99.90, -1.00),
        ],
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    )

    df = df.withColumn(
        "shipping_limit_date", df["shipping_limit_date"].cast("timestamp")
    )

    with pytest.raises(ValueError):
        validate_order_items(df)
