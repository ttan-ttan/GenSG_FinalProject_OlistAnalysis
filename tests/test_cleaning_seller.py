
# pylint: disable=redefined-outer-name
# pylint: disable=wrong-import-order
# pylint: disable=unused-import
# pylint: disable=no-member

"""
Test Suite: Cleaning Logic for Sellers Dataset

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
"""

from src.cleaning_sellers import clean_sellers
from pyspark.sql import SparkSession
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
        [("s1", "12345", " Sao Paulo ", "sp")],
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    )


@pytest.fixture
def df_valid_zip(spark):
    """Valid ZIP code row."""
    return spark.createDataFrame(
        [("s2", "13056", "campinas", "SP")],
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    )


@pytest.fixture
def df_invalid_zip(spark):
    """Invalid ZIP code row."""
    return spark.createDataFrame(
        [("s3", "999", "campinas", "SP")],
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    )


def test_clean_sellers_types(df_mixed_types):
    """Ensure columns are cast to correct types."""
    cleaned = clean_sellers(df_mixed_types)

    assert cleaned.schema["seller_zip_code_prefix"].dataType.typeName(
    ) == "integer"
    assert cleaned.schema["seller_city"].dataType.typeName() == "string"


def test_clean_sellers_city_state_format(df_mixed_types):
    """Ensure city is lowercase and state is uppercase."""
    cleaned = clean_sellers(df_mixed_types)
    row = cleaned.first()

    assert row["seller_city"] == "sao paulo"
    assert row["seller_state"] == "SP"


def test_clean_sellers_zip_validation(df_valid_zip, df_invalid_zip):
    """Ensure invalid zip codes are removed."""
    df = df_invalid_zip.union(df_valid_zip)
    cleaned = clean_sellers(df)

    assert cleaned.count() == 1
    assert cleaned.first()["seller_id"] == "s2"


def test_clean_sellers_basic(spark):
    """End-to-end cleaning test with mixed valid/invalid rows."""
    df = spark.createDataFrame(
        [
            ("s1", "12345", " Sao Paulo ", "sp"),
            ("s2", "50", "Campinas", "SP"),  # invalid ZIP
        ],
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]
    )

    cleaned = clean_sellers(df)
    assert cleaned.count() == 1
    assert cleaned.first()["seller_id"] == "s1"
