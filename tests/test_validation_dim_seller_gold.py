"""Tests for Gold seller dimension validation."""

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.validation_dim_seller_gold import validate_dim_seller_gold


def test_gold_valid(spark):
    """Valid seller records should pass gold validation."""
    df = spark.createDataFrame(
        [
            ("S001", "sao paulo", "SP", 12345),
            ("S002", "campinas", "SP", 13056),
        ],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"],
    )
    out = validate_dim_seller_gold(df)
    assert out.count() == 2


def test_gold_duplicate_id(spark):
    """Duplicate seller IDs should fail validation."""
    df = spark.createDataFrame(
        [
            ("S001", "sao paulo", "SP", 12345),
            ("S001", "campinas", "SP", 13056),
        ],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"],
    )
    with pytest.raises(ValueError):
        validate_dim_seller_gold(df)


def test_gold_invalid_state(spark):
    """Invalid state codes should fail validation."""
    df = spark.createDataFrame(
        [("S001", "sao paulo", "XX", 12345)],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"],
    )
    with pytest.raises(ValueError):
        validate_dim_seller_gold(df)


def test_gold_invalid_zip_prefix(spark):
    """ZIP prefixes below the cleaner's valid range should fail validation."""
    df = spark.createDataFrame(
        [("S001", "sao paulo", "SP", 999)],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"],
    )
    with pytest.raises(ValueError, match="seller_zip_code_prefix"):
        validate_dim_seller_gold(df)


def test_gold_rejects_zip_prefix_above_maximum(spark):
    """ZIP prefixes above the valid range should fail validation."""
    df = spark.createDataFrame(
        [("S001", "sao paulo", "SP", 100000)],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"],
    )
    with pytest.raises(ValueError, match="seller_zip_code_prefix"):
        validate_dim_seller_gold(df)


def test_gold_rejects_unnormalized_city(spark):
    """Gold city values should match the cleaner's lowercase trimmed format."""
    df = spark.createDataFrame(
        [("S001", " Sao Paulo ", "SP", 12345)],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"],
    )
    with pytest.raises(ValueError, match="seller_city is not normalized"):
        validate_dim_seller_gold(df)


def test_gold_missing_required_column(spark):
    """Missing columns should produce a clear validation error."""
    df = spark.createDataFrame(
        [("S001", "sao paulo", "SP")], ["seller_id", "seller_city", "seller_state"]
    )
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_dim_seller_gold(df)


def test_gold_null_critical_fields(spark):
    """Null critical fields should fail validation."""
    schema = StructType(
        [
            StructField("seller_id", StringType(), True),
            StructField("seller_city", StringType(), True),
            StructField("seller_state", StringType(), True),
            StructField("seller_zip_code_prefix", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(
        [("S001", None, "SP", 12345)],
        schema,
    )
    with pytest.raises(ValueError):
        validate_dim_seller_gold(df)
