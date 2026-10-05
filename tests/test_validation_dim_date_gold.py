""" TESTS for validation_dim_date_gold.py   """

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.validation_dim_date_gold import validate_dim_date_gold

DIM_DATE_SCHEMA = StructType(
    [
        StructField("date_key", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("dow", IntegerType(), True),
        StructField("is_black_friday", BooleanType(), True),
        StructField("event_window", BooleanType(), True),
        StructField("is_weekend", BooleanType(), True),
    ]
)


def make_valid_dim(spark):
    """Create a valid DataFrame for testing validate_dim_date_gold."""
    return spark.createDataFrame(
        [
            (20180101, "2018-01-01", 2018, 1, 2, False, False, False),
            (20180102, "2018-01-02", 2018, 1, 3, False, False, False),
        ],
        DIM_DATE_SCHEMA,
    )


def test_valid_dim_date_gold_passes(spark):
    """Test that validate_dim_date_gold accepts a valid DataFrame without errors."""
    out = validate_dim_date_gold(make_valid_dim(spark))
    assert out.count() == 2


def test_dim_date_gold_rejects_duplicate_date_key(spark):
    """A date key must identify exactly one calendar day."""
    df = make_valid_dim(spark).union(
        spark.createDataFrame(
            [(20180101, "2018-01-03", 2018, 1, 4, False, False, False)],
            DIM_DATE_SCHEMA,
        )
    )
    with pytest.raises(ValueError):
        validate_dim_date_gold(df)


def test_dim_date_gold_rejects_null_or_invalid_date(spark):
    """Reject null and invalid date values."""
    df = spark.createDataFrame(
        [(None, None, 2018, 1, 1, False, False, False)],
        schema=DIM_DATE_SCHEMA,
    )
    with pytest.raises(ValueError):
        validate_dim_date_gold(df)


def test_dim_date_gold_rejects_impossible_calendar_date(spark):
    """Reject strings that match the date pattern but are not real dates."""
    df = make_valid_dim(spark).withColumn("date", F.lit("2018-02-30"))
    with pytest.raises(ValueError, match="Invalid date value"):
        validate_dim_date_gold(df)


def test_dim_date_gold_rejects_inconsistent_date_attributes(spark):
    """Reject dimension attributes that do not agree with calendar_date."""
    df = make_valid_dim(spark).withColumn("month", F.lit(2))
    with pytest.raises(ValueError, match="Inconsistent month"):
        validate_dim_date_gold(df)
