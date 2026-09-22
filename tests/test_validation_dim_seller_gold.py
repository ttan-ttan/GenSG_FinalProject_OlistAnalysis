"""Tests for Gold seller dimension validation."""

import pytest
from src.validation_dim_seller_gold import validate_dim_seller_gold
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def test_gold_valid(spark):
    """Valid seller records should pass gold validation."""
    df = spark.createDataFrame(
        [
            ("S001", "sao paulo", "SP", 12345),
            ("S002", "campinas", "SP", 13056),
        ],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"]
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
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"]
    )
    with pytest.raises(ValueError):
        validate_dim_seller_gold(df)


def test_gold_invalid_state(spark):
    """Invalid state codes should fail validation."""
    df = spark.createDataFrame(
        [("S001", "sao paulo", "XX", 12345)],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"]
    )
    with pytest.raises(ValueError):
        validate_dim_seller_gold(df)


def test_gold_null_critical_fields(spark):
    """Null critical fields should fail validation."""
    schema = StructType([
        StructField("seller_id", StringType(), True),
        StructField("seller_city", StringType(), True),
        StructField("seller_state", StringType(), True),
        StructField("seller_zip_code_prefix", IntegerType(), True),
    ])
    df = spark.createDataFrame(
        [("S001", None, "SP", 12345)],
        schema,
    )
    with pytest.raises(ValueError):
        validate_dim_seller_gold(df)
