""" TESTS for validation_fact_order_item_gold.py """

import pytest

from src.validation_fact_order_item_gold import validate_fact_order_item_gold


def make_valid_df(spark):
    """Create a valid DataFrame for testing validate_fact_order_item_gold."""
    return spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", "2018-01-01 00:00:00", 10.5, 2.0),
            ("o2", 2, "p2", "s2", "2018-01-02 00:00:00", 20.0, 4.5),
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


def test_validate_fact_order_item_gold_accepts_valid_data(spark):
    """Test that validate_fact_order_item_gold accepts a valid DataFrame without errors."""
    out = validate_fact_order_item_gold(make_valid_df(spark))
    assert out.count() == 2


def test_validate_fact_order_item_gold_rejects_null_keys(spark):
    """Test that validate_fact_order_item_gold raises an error for null values in key columns."""
    df = make_valid_df(spark).withColumn("seller_id", None)
    with pytest.raises(ValueError):
        validate_fact_order_item_gold(df)


def test_validate_fact_order_item_gold_rejects_duplicate_order_item(spark):
    """Test that validate_fact_order_item_gold raises an error for duplicate
    order_id and order_item_id combinations."""
    df = make_valid_df(spark).union(
        spark.createDataFrame(
            [("o1", 1, "p1", "s1", "2018-01-01 00:00:00", 10.5, 2.0)],
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
    )
    with pytest.raises(ValueError):
        validate_fact_order_item_gold(df)
