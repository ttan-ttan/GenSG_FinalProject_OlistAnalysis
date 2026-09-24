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


from src.validation_dim_customer_gold import validate_dim_customer_gold
import pytest
from pyspark.sql.types import StringType, StructField, StructType


def test_gold_valid(spark):
    """Ensure valid customer records pass gold validation without errors."""
    df = spark.createDataFrame(
        [
            ("C001", "sao paulo", "SP", "2020-01-01"),
            ("C002", "campinas", "SP", "2021-05-10"),
        ],
        [
            "customer_id",
            "customer_city",
            "customer_state",
            "customer_first_purchase_date",
        ],
    )
    out = validate_dim_customer_gold(df)
    assert out.count() == 2


def test_gold_duplicate_id(spark):
    """Ensure validation fails when duplicate customer_id values exist."""
    df = spark.createDataFrame(
        [
            ("C001", "sao paulo", "SP", "2020-01-01"),
            ("C001", "campinas", "SP", "2021-05-10"),
        ],
        [
            "customer_id",
            "customer_city",
            "customer_state",
            "customer_first_purchase_date",
        ],
    )
    with pytest.raises(ValueError):
        validate_dim_customer_gold(df)


def test_gold_invalid_state(spark):
    """Ensure validation fails when customer_state is not a valid Brazilian state."""
    df = spark.createDataFrame(
        [("C001", "sao paulo", "XX", "2020-01-01")],
        [
            "customer_id",
            "customer_city",
            "customer_state",
            "customer_first_purchase_date",
        ],
    )
    with pytest.raises(ValueError):
        validate_dim_customer_gold(df)


def test_gold_future_date(spark):
    """Ensure validation fails when first_purchase_date is in the future."""
    df = spark.createDataFrame(
        [("C001", "sao paulo", "SP", "2999-01-01")],
        [
            "customer_id",
            "customer_city",
            "customer_state",
            "customer_first_purchase_date",
        ],
    )
    with pytest.raises(ValueError):
        validate_dim_customer_gold(df)


def test_gold_null_critical_fields(spark):
    """Ensure validation fails when any critical field is null."""
    schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("customer_city", StringType(), True),
            StructField("customer_state", StringType(), True),
            StructField("customer_first_purchase_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(
        [("C001", None, "SP", "2020-01-01")],
        schema,
    )
    with pytest.raises(ValueError):
        validate_dim_customer_gold(df)
