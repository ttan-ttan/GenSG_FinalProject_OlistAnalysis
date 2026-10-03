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
        StructField("calendar_date", StringType(), True),
        StructField("year_num", IntegerType(), True),
        StructField("month_num", IntegerType(), True),
        StructField("month_name", StringType(), True),
        StructField("day_num", IntegerType(), True),
        StructField("day_of_week_num", IntegerType(), True),
        StructField("is_weekend", BooleanType(), True),
    ]
)


def make_valid_dim(spark):
    """Create a valid DataFrame for testing validate_dim_date_gold."""
    return spark.createDataFrame(
        [
            ("2018-01-01", 2018, 1, "January", 1, 2, False),
            ("2018-01-02", 2018, 1, "January", 2, 3, False),
        ],
        [
            "calendar_date",
            "year_num",
            "month_num",
            "month_name",
            "day_num",
            "day_of_week_num",
            "is_weekend",
        ],
    )


def test_valid_dim_date_gold_passes(spark):
    """Test that validate_dim_date_gold accepts a valid DataFrame without errors."""
    out = validate_dim_date_gold(make_valid_dim(spark))
    assert out.count() == 2


def test_dim_date_gold_rejects_duplicate_calendar_date(spark):
    """Test that validate_dim_date_gold raises an error for duplicate calendar_date values."""
    df = make_valid_dim(spark).union(
        spark.createDataFrame(
            [("2018-01-01", 2018, 1, "January", 1, 1, False)],
            [
                "calendar_date",
                "year_num",
                "month_num",
                "month_name",
                "day_num",
                "day_of_week_num",
                "is_weekend",
            ],
        )
    )
    with pytest.raises(ValueError):
        validate_dim_date_gold(df)


def test_dim_date_gold_rejects_null_or_invalid_date(spark):
    """Test that validate_dim_date_gold raises an error for null or
    invalid calendar_date values."""
    df = spark.createDataFrame(
        [(None, 2018, 1, "January", 1, 1, False)],
        schema=DIM_DATE_SCHEMA,
    )
    with pytest.raises(ValueError):
        validate_dim_date_gold(df)


def test_dim_date_gold_rejects_impossible_calendar_date(spark):
    """Reject strings that match the date pattern but are not real dates."""
    df = make_valid_dim(spark).withColumn("calendar_date", F.lit("2018-02-30"))
    with pytest.raises(ValueError, match="Invalid calendar_date value"):
        validate_dim_date_gold(df)


def test_dim_date_gold_rejects_inconsistent_date_attributes(spark):
    """Reject dimension attributes that do not agree with calendar_date."""
    df = make_valid_dim(spark).withColumn("month_num", F.lit(2))
    with pytest.raises(ValueError, match="Inconsistent month_num"):
        validate_dim_date_gold(df)
