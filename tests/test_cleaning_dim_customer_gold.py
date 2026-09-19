"""
Test Suite: Gold validation for customers dimension.
"""

import pytest
from pyspark.sql import SparkSession
from src.validation_customers_gold import validate_customers_gold


def test_gold_valid(spark):
    """Ensure valid customer records pass gold validation without errors."""
    df = spark.createDataFrame(
        [
            ("C001", "sao paulo", "SP", "2020-01-01"),
            ("C002", "campinas", "SP", "2021-05-10"),
        ],
        ["customer_id", "customer_city", "customer_state",
            "customer_first_purchase_date"]
    )

    out = validate_customers_gold(df)
    assert out.count() == 2


def test_gold_duplicate_id(spark):
    """Ensure validation fails when duplicate customer_id values exist."""
    df = spark.createDataFrame(
        [
            ("C001", "sao paulo", "SP", "2020-01-01"),
            ("C001", "campinas", "SP", "2021-05-10"),
        ],
        ["customer_id", "customer_city", "customer_state",
            "customer_first_purchase_date"]
    )

    with pytest.raises(ValueError):
        validate_customers_gold(df)


def test_gold_invalid_state(spark):
    """Ensure validation fails when customer_state is not a valid Brazilian state."""
    df = spark.createDataFrame(
        [("C001", "sao paulo", "XX", "2020-01-01")],
        ["customer_id", "customer_city", "customer_state",
            "customer_first_purchase_date"]
    )

    with pytest.raises(ValueError):
        validate_customers_gold(df)


def test_gold_future_date(spark):
    """Ensure validation fails when first_purchase_date is in the future."""
    df = spark.createDataFrame(
        [("C001", "sao paulo", "SP", "2999-01-01")],
        ["customer_id", "customer_city", "customer_state",
            "customer_first_purchase_date"]
    )

    with pytest.raises(ValueError):
        validate_customers_gold(df)
