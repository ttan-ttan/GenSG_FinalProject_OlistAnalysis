"""
Test Suite: Gold Validation for Customers Dimension
"""

import pytest
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


def test_gold_future_date(spark):
    """Ensure validation fails when first_purchase_date is in the future."""
    df = spark.createDataFrame(
        [("C001", "sao paulo", "SP", "2999-01-01")],
        ["customer_id", "customer_city", "customer_state",
            "customer_first_purchase_date"]
    )
    with pytest.raises(ValueError):
        validate_customers_gold(df)
