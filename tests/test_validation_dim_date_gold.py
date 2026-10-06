"""TESTS for validation_dim_date_gold.py"""

import pytest
from pyspark.sql import functions as F

from src.cleaning_dim_date_gold import build_dim_date
from src.validation_dim_date_gold import validate_dim_date_gold


def make_valid_dim(spark):
    """Small valid dimension around Black Friday 2017."""
    return build_dim_date(spark, "2017-11-20", "2017-11-30")


def test_valid_dim_date_gold_passes(spark):
    out = validate_dim_date_gold(make_valid_dim(spark))
    assert out.count() == 11


def test_full_calendar_passes(spark):
    out = validate_dim_date_gold(build_dim_date(spark))
    assert out.count() == 1096


def test_rejects_missing_column(spark):
    df = make_valid_dim(spark).drop("event_period")
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_dim_date_gold(df)


def test_rejects_duplicate_date_key(spark):
    df = make_valid_dim(spark)
    df = df.union(df.limit(1))
    with pytest.raises(ValueError, match="Duplicate date_key"):
        validate_dim_date_gold(df)


def test_rejects_null_date(spark):
    df = make_valid_dim(spark).withColumn(
        "date", F.when(F.col("date_key") == 20171120, None).otherwise(F.col("date"))
    )
    with pytest.raises(ValueError, match="date contains null"):
        validate_dim_date_gold(df)


def test_rejects_impossible_calendar_date(spark):
    df = make_valid_dim(spark).withColumn("date", F.lit("2018-02-30"))
    with pytest.raises(ValueError):
        validate_dim_date_gold(df)


def test_rejects_inconsistent_month(spark):
    df = make_valid_dim(spark).withColumn("month", F.lit(2))
    with pytest.raises(ValueError, match="Inconsistent month"):
        validate_dim_date_gold(df)


def test_rejects_wrong_label(spark):
    """Labels must match exactly what the semantic model filters on."""
    df = make_valid_dim(spark).withColumn(
        "event_period",
        F.when(F.col("date_key") == 20171124, "BF").otherwise(F.col("event_period")),
    )
    with pytest.raises(ValueError, match="Inconsistent event_period"):
        validate_dim_date_gold(df)


def test_rejects_missing_days(spark):
    df = make_valid_dim(spark).filter("date_key != 20171125")
    with pytest.raises(ValueError, match="missing days"):
        validate_dim_date_gold(df)
