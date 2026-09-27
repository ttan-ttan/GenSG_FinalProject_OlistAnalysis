# pylint: disable=redefined-outer-name
# pylint: disable=wrong-import-order
# pylint: disable=no-member

"""
Test Suite: Validation Logic for Sellers Dataset
"""

import os
import sys

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.validation_sellers import validate_sellers

# Add src folder to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))


def test_validate_sellers_valid(spark):
    """Ensure valid seller records pass validation without errors."""
    df = spark.createDataFrame(
        [
            ("S001", 1234, "sao paulo", "SP"),
            ("S002", 99999, "rio de janeiro", "RJ"),
        ],
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    )

    df_val = validate_sellers(df)
    assert df_val.count() == 2


def test_validate_sellers_null_id(spark):
    """Ensure validation fails when seller_id is null."""
    schema = StructType(
        [
            StructField("seller_id", StringType(), True),
            StructField("seller_zip_code_prefix", IntegerType(), True),
            StructField("seller_city", StringType(), True),
            StructField("seller_state", StringType(), True),
        ]
    )
    df = spark.createDataFrame(
        [(None, 1234, "campinas", "SP")],
        schema,
    )

    with pytest.raises(ValueError):
        validate_sellers(df)


def test_validate_sellers_duplicate_id(spark):
    """Ensure validation fails when duplicate seller_id values exist."""
    df = spark.createDataFrame(
        [
            ("S001", 1234, "sao paulo", "SP"),
            ("S001", 5000, "campinas", "SP"),
        ],
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    )

    with pytest.raises(ValueError):
        validate_sellers(df)


def test_validate_sellers_invalid_zip(spark):
    """Ensure validation fails when ZIP code prefix is outside the valid range."""
    df = spark.createDataFrame(
        [("S001", 50, "sao paulo", "SP")],
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    )

    with pytest.raises(ValueError):
        validate_sellers(df)


def test_validate_sellers_invalid_state(spark):
    """Ensure validation fails when seller_state is not a valid 2‑letter code."""
    df = spark.createDataFrame(
        [("S001", 1234, "sao paulo", "XX")],
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    )

    with pytest.raises(ValueError):
        validate_sellers(df)
