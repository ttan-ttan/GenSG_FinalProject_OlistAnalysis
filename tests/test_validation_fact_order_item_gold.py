""" TESTS for validation_fact_order_item_gold.py """

import pytest
from pyspark.sql import functions as F

from src.validation_fact_order_item_gold import validate_fact_order_item_gold


def make_valid_df(spark):
    """Create a valid DataFrame for testing validate_fact_order_item_gold."""
    return spark.createDataFrame(
        [
            (1, "o1", 20180101, 11, 21, 31, 10.5, 2.0, 25.0),
            (2, "o2", 20180102, 12, 22, 32, 20.0, 4.5, None),
        ],
        [
            "order_item_id",
            "order_id",
            "date_key",
            "customer_key",
            "product_key",
            "seller_key",
            "price",
            "freight_value",
            "price_vs_baseline_pct",
        ],
    )


def test_validate_fact_order_item_gold_accepts_valid_data(spark):
    """Test that validate_fact_order_item_gold accepts a valid DataFrame without errors."""
    out = validate_fact_order_item_gold(make_valid_df(spark))
    assert out.count() == 2


def test_validate_fact_order_item_gold_rejects_null_keys(spark):
    """Test that validate_fact_order_item_gold raises an error for null values in key columns."""
    df = make_valid_df(spark).withColumn("seller_key", F.lit(None))
    with pytest.raises(ValueError):
        validate_fact_order_item_gold(df)


@pytest.mark.parametrize("column", ["price", "freight_value"])
def test_validate_fact_order_item_gold_rejects_null_amounts(spark, column):
    """Null item amount fields should not pass their range checks."""
    df = make_valid_df(spark).withColumn(column, F.lit(None).cast("double"))
    with pytest.raises(ValueError, match="Null critical field"):
        validate_fact_order_item_gold(df)


@pytest.mark.parametrize("column", ["price", "freight_value"])
def test_validate_fact_order_item_gold_rejects_nan_amounts(spark, column):
    """NaN amounts should not pass numeric range checks."""
    df = make_valid_df(spark).withColumn(column, F.lit(float("nan")))
    with pytest.raises(ValueError, match="Invalid"):
        validate_fact_order_item_gold(df)


@pytest.mark.parametrize("column", ["price", "freight_value"])
def test_validate_fact_order_item_gold_rejects_infinite_amounts(spark, column):
    """Infinite amounts should fail validation."""
    df = make_valid_df(spark).withColumn(column, F.lit(float("inf")))
    with pytest.raises(ValueError, match="Invalid"):
        validate_fact_order_item_gold(df)


def test_validate_fact_order_item_gold_rejects_nonpositive_item_id(spark):
    """Order-item IDs should be positive within each order."""
    df = make_valid_df(spark).withColumn("order_item_id", F.lit(0))
    with pytest.raises(ValueError, match="order_item_id"):
        validate_fact_order_item_gold(df)


def test_validate_fact_order_item_gold_rejects_duplicate_order_item(spark):
    """Test that validate_fact_order_item_gold raises an error for duplicate
    order_id and order_item_id combinations."""
    valid_df = make_valid_df(spark)
    df = valid_df.union(valid_df.limit(1))
    with pytest.raises(ValueError):
        validate_fact_order_item_gold(df)
