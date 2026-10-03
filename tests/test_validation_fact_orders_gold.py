""" TESTS for validation_fact_orders_gold.py """

import pytest
from pyspark.sql import functions as F

from src.validation_fact_orders_gold import validate_fact_orders_gold


VALID_COLUMNS = [
    "order_id",
    "date_key",
    "customer_key",
    "order_value",
    "item_count",
    "delivery_delay_days",
    "payment_value_total",
    "is_new_at_order",
]


def make_valid_df(spark):
    """Create a valid DataFrame for testing validate_fact_orders_gold."""
    return spark.createDataFrame(
        [
            ("ord_001", 20240101, 1, 32.5, 2, 1, 32.5, True),
            ("ord_002", 20240201, 2, 0.0, 0, None, 0.0, False),
        ],
        VALID_COLUMNS,
    )


def test_validate_fact_orders_gold_accepts_valid_data(spark):
    """Test that validate_fact_orders_gold accepts a valid DataFrame without errors."""
    df = make_valid_df(spark)
    out = validate_fact_orders_gold(df)
    assert out.count() == 2


def test_validate_fact_orders_gold_rejects_duplicate_order_ids(spark):
    """Test that validate_fact_orders_gold raises an error for duplicate order_id values."""
    df = make_valid_df(spark)
    dup = df.union(df.limit(1).withColumn("order_id", df["order_id"]))
    with pytest.raises(ValueError):
        validate_fact_orders_gold(dup)


def test_validate_fact_orders_gold_rejects_negative_order_value(spark):
    """Order-level monetary measures cannot be negative."""
    df = make_valid_df(spark)
    bad = df.withColumn("order_value", F.lit(-1.0))
    with pytest.raises(ValueError):
        validate_fact_orders_gold(bad)


def test_validate_fact_orders_gold_rejects_null_key_fields(spark):
    """Test that validate_fact_orders_gold raises an error for null values in key columns."""
    df = make_valid_df(spark)
    bad = df.withColumn("customer_key", F.lit(None))
    with pytest.raises(ValueError):
        validate_fact_orders_gold(bad)


def test_validate_fact_orders_gold_rejects_null_order_grain_key(spark):
    df = make_valid_df(spark).withColumn("order_id", F.lit(None))
    with pytest.raises(ValueError):
        validate_fact_orders_gold(df)
