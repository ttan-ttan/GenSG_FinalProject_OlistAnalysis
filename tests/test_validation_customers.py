# pylint: disable=redefined-outer-name
# pylint: disable=wrong-import-order
# pylint: disable=no-member

"""
Test Suite: Validation Logic for Customers Dataset
"""

from src.validation_customers import validate_customers
from pyspark.sql import SparkSession
import pytest
import sys
import os

# Add src folder to Python path
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "src")))


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for this test module."""
    return SparkSession.builder.getOrCreate()


def test_validate_customers_valid(spark):
    """Ensure valid customer records pass validation without errors."""
    df = spark.createDataFrame(
        [
            ("C001", "U001", 1234, "sao paulo", "SP"),
            ("C002", "U002", 99999, "rio de janeiro", "RJ"),
        ],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"]
    )

    df_val = validate_customers(df)
    assert df_val.count() == 2


def test_validate_customers_invalid_state(spark):
    """Ensure validation fails when customer_state is not a valid 2‑letter code."""
    df = spark.createDataFrame(
        [("C001", "U001", 1234, "sao paulo", "XX")],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"]
    )

    with pytest.raises(ValueError):
        validate_customers(df)


def test_validate_customers_duplicate_id(spark):
    """Ensure validation fails when duplicate customer_id values exist."""
    df = spark.createDataFrame(
        [
            ("C001", "U001", 1234, "sao paulo", "SP"),
            ("C001", "U002", 5000, "campinas", "SP"),
        ],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"]
    )

    with pytest.raises(ValueError):
        validate_customers(df)


def test_validate_customers_invalid_zip(spark):
    """Ensure validation fails when ZIP code prefix is outside the valid range."""
    df = spark.createDataFrame(
        [("C001", "U001", 50, "sao paulo", "SP")],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"]
    )

    with pytest.raises(ValueError):
        validate_customers(df)
