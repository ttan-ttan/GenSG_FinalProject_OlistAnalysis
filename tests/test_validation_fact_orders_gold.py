""" TESTS for validation_fact_orders_gold.py """

import pytest
from pyspark.sql import functions as F

from src.validation_fact_orders_gold import validate_fact_orders_gold


VALID_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def make_valid_df(spark):
    """Create a valid DataFrame for testing validate_fact_orders_gold."""
    return spark.createDataFrame(
        [
            (
                "ord_001",
                "cust_001",
                "delivered",
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-02 10:00:00",
                "2024-01-03 10:00:00",
                "2024-01-05 10:00:00",
            ),
            (
                "ord_002",
                "cust_002",
                "processing",
                "2024-02-01 12:00:00",
                None,
                None,
                None,
                "2024-02-05 12:00:00",
            ),
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


def test_validate_fact_orders_gold_rejects_invalid_status(spark):
    """Test that validate_fact_orders_gold raises an error for invalid order_status values."""
    df = make_valid_df(spark)
    bad = df.withColumn("order_status", F.lit("unknown"))
    with pytest.raises(ValueError):
        validate_fact_orders_gold(bad)


def test_validate_fact_orders_gold_rejects_null_key_fields(spark):
    """Test that validate_fact_orders_gold raises an error for null values in key columns."""
    df = make_valid_df(spark)
    bad = df.withColumn("customer_id", F.lit(None))
    with pytest.raises(ValueError):
        validate_fact_orders_gold(bad)


def test_validate_fact_orders_gold_rejects_impossible_date_order(spark):
    """Test that validate_fact_orders_gold raises an error for impossible date orders."""
    df = make_valid_df(spark)
    bad = df.withColumn(
        "order_approved_at",
        df["order_purchase_timestamp"].cast("string"),
    )
    bad = bad.withColumn(
        "order_approved_at",
        bad["order_approved_at"].substr(1, 10),
    )
    with pytest.raises(ValueError):
        validate_fact_orders_gold(bad)
