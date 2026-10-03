# pylint: disable=redefined-outer-name
# pylint: disable=wrong-import-order
# pylint: disable=no-member

"""
Test Suite: Gold validation for customers dimension.

Purpose:
    Ensure Gold-layer dimensional integrity and business rule correctness.
    These tests validate:
        - Unique customer_id
        - Valid Brazilian state codes
        - Logical first_purchase_date (not future)
        - Critical non-null fields
"""

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.validation_dim_customer_gold import validate_dim_customer_gold


DIM_COLUMNS = [
    "customer_key",
    "customer_unique_id",
    "first_order_date",
    "state",
    "city",
]
DIM_SCHEMA = StructType(
    [
        StructField("customer_key", IntegerType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("first_order_date", StringType(), True),
        StructField("state", StringType(), True),
        StructField("city", StringType(), True),
    ]
)


def _dim(spark, rows):
    return spark.createDataFrame(rows, DIM_SCHEMA)


def test_gold_valid(spark):
    """Ensure valid customer records pass gold validation without errors."""
    df = _dim(
        spark,
        [
            (1, "U001", "2020-01-01", "SP", "sao paulo"),
            (2, "U002", "2021-05-10", "SP", "campinas"),
        ],
    )
    out = validate_dim_customer_gold(df)
    assert out.count() == 2


def test_gold_duplicate_id(spark):
    """Ensure validation fails when duplicate customer_id values exist."""
    df = _dim(
        spark,
        [
            (1, "U001", "2020-01-01", "SP", "sao paulo"),
            (2, "U001", "2021-05-10", "SP", "campinas"),
        ],
    )
    with pytest.raises(ValueError):
        validate_dim_customer_gold(df)


def test_gold_invalid_state(spark):
    """Ensure validation fails when customer_state is not a valid Brazilian state."""
    df = _dim(spark, [(1, "U001", "2020-01-01", "XX", "sao paulo")])
    with pytest.raises(ValueError):
        validate_dim_customer_gold(df)


def test_gold_future_date(spark):
    """Ensure validation fails when first_purchase_date is in the future."""
    df = _dim(spark, [(1, "U001", "2999-01-01", "SP", "sao paulo")])
    with pytest.raises(ValueError):
        validate_dim_customer_gold(df)


def test_gold_null_critical_fields(spark):
    """Ensure validation fails when any critical field is null."""
    df = _dim(spark, [(1, "U001", "2020-01-01", "SP", None)])
    with pytest.raises(ValueError):
        validate_dim_customer_gold(df)


def test_gold_missing_required_column(spark):
    """Missing columns should produce a clear validation error."""
    df = spark.createDataFrame(
        [(1, "U001", "2020-01-01", "SP")],
        ["customer_key", "customer_unique_id", "first_order_date", "state"],
    )
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_dim_customer_gold(df)


def test_gold_invalid_first_purchase_date(spark):
    """Unparseable first-purchase dates should not pass validation."""
    df = _dim(spark, [(1, "U001", "not-a-date", "SP", "sao paulo")])
    with pytest.raises(ValueError, match="Invalid first_order_date"):
        validate_dim_customer_gold(df)


def test_gold_requires_normalized_city(spark):
    """Gold city values should match the cleaner's lowercase trimmed format."""
    df = _dim(spark, [(1, "U001", "2020-01-01", "SP", " Sao Paulo ")])
    with pytest.raises(ValueError, match="city is not normalized"):
        validate_dim_customer_gold(df)
