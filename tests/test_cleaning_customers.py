# pylint: disable=redefined-outer-name

"""
Test Suite: Cleaning Logic for Customers Dataset

Purpose:
    Validate the cleaning rules applied in the Silver layer transformation.
    These tests ensure that the cleaning module behaves consistently and
    prevents regressions when new logic is added.

Covers:
    1. Type casting
    2. City/state normalization
    3. Zip code validation
    4. Removal of invalid rows
    5. Basic formatting cleanup

Notes:
    - Tests run locally using PySpark.
    - Fabric notebooks will import the same cleaning module.
"""

# pylint: disable=wrong-import-order
# pylint: disable=unused-import
# pylint: disable=no-member

from src.cleaning_customers import clean_customers
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pytest
import sys
import os

# Add src to PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "src")))


@pytest.fixture
def df_mixed_types(spark):
    """Sample DF for type casting + city/state normalization tests."""
    return spark.createDataFrame(
        [("id1", "uid1", "12345", "sao paulo", "sp")],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"],
    )


@pytest.fixture
def df_valid_zip(spark):
    """Valid ZIP code row."""
    return spark.createDataFrame(
        [("id2", "uid2", "13056", "campinas", "SP")],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"],
    )


@pytest.fixture
def df_invalid_zip(spark):
    """Invalid ZIP code row."""
    return spark.createDataFrame(
        [("id1", "uid1", "999", "campinas", "SP")],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"],
    )


def test_clean_customers_types(df_mixed_types):
    """Ensure columns are cast to correct types."""
    cleaned = clean_customers(df_mixed_types)

    assert cleaned.schema["customer_zip_code_prefix"].dataType.typeName(
    ) == "integer"
    assert cleaned.schema["customer_city"].dataType.typeName() == "string"


def test_clean_customers_city_state_format(df_mixed_types):
    """Ensure city is lowercase and state is uppercase."""
    cleaned = clean_customers(df_mixed_types)
    row = cleaned.first()

    assert row["customer_city"] == "sao paulo"
    assert row["customer_state"] == "SP"


def test_clean_customers_zip_validation(df_valid_zip, df_invalid_zip):
    """Ensure invalid zip codes are removed."""
    df = df_invalid_zip.union(df_valid_zip)
    cleaned = clean_customers(df)

    assert cleaned.count() == 1
    assert cleaned.first()["customer_id"] == "id2"


def test_clean_customers_basic(spark):
    """End-to-end cleaning test with mixed valid/invalid rows."""
    df = spark.createDataFrame(
        [
            ("id1", "uid1", "12345", " Sao Paulo ", "sp"),
            ("id2", None, "12345", "Campinas", "SP"),  # invalid unique_id
        ],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"]
    )

    cleaned = clean_customers(df)
    assert cleaned.count() == 1
    assert cleaned.first()["customer_id"] == "id1"
