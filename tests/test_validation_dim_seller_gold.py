"""Tests for Gold seller dimension validation."""

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.validation_dim_seller_gold import validate_dim_seller_gold

SELLER_SCHEMA = StructType(
    [
        StructField("seller_key", IntegerType(), True),
        StructField("seller_id", StringType(), True),
        StructField("seller_state", StringType(), True),
        StructField("seller_city", StringType(), True),
    ]
)


def _dim(spark, rows):
    return spark.createDataFrame(rows, SELLER_SCHEMA)


def test_gold_valid(spark):
    """Valid seller records should pass gold validation."""
    df = _dim(
        spark,
        [(1, "S001", "SP", "sao paulo"), (2, "S002", "SP", "campinas")],
    )
    out = validate_dim_seller_gold(df)
    assert out.count() == 2


def test_gold_duplicate_id(spark):
    """Duplicate seller IDs should fail validation."""
    df = _dim(
        spark,
        [(1, "S001", "SP", "sao paulo"), (2, "S001", "SP", "campinas")],
    )
    with pytest.raises(ValueError):
        validate_dim_seller_gold(df)


def test_gold_invalid_state(spark):
    """Invalid state codes should fail validation."""
    df = _dim(spark, [(1, "S001", "XX", "sao paulo")])
    with pytest.raises(ValueError):
        validate_dim_seller_gold(df)


def test_gold_duplicate_seller_key(spark):
    """The surrogate key must identify exactly one seller."""
    df = _dim(
        spark,
        [(1, "S001", "SP", "sao paulo"), (1, "S002", "SP", "campinas")],
    )
    with pytest.raises(ValueError, match="Duplicate seller_key"):
        validate_dim_seller_gold(df)


def test_gold_rejects_unnormalized_city(spark):
    """Gold city values should match the cleaner's lowercase trimmed format."""
    df = _dim(spark, [(1, "S001", "SP", " Sao Paulo ")])
    with pytest.raises(ValueError, match="seller_city is not normalized"):
        validate_dim_seller_gold(df)


def test_gold_missing_required_column(spark):
    """Missing columns should produce a clear validation error."""
    df = spark.createDataFrame(
        [(1, "S001", "SP")], ["seller_key", "seller_id", "seller_state"]
    )
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_dim_seller_gold(df)


def test_gold_null_critical_fields(spark):
    """Null critical fields should fail validation."""
    df = _dim(spark, [(1, "S001", "SP", None)])
    with pytest.raises(ValueError):
        validate_dim_seller_gold(df)
