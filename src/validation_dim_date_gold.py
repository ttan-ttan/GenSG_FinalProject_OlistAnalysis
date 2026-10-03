""" validation_dim_date_gold
This module contains functions to validate the Gold date dimension table."""

from __future__ import annotations

from operator import invert

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "date_key",
    "date",
    "year",
    "month",
    "dow",
    "is_black_friday",
    "event_window",
    "is_weekend",
]


def validate_dim_date_gold(df: DataFrame) -> DataFrame:
    """Validate the Gold date dimension."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    for col in REQUIRED_COLUMNS:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"{col} contains null values")

    dup = df.groupBy("date_key").count().filter(F.col("count") > 1)
    if dup.count() > 0:
        raise ValueError("Duplicate date_key values detected")
    dup_dates = df.groupBy("date").count().filter(F.col("count") > 1)
    if dup_dates.count() > 0:
        raise ValueError("Duplicate date values detected")

    date_text = F.col("date").cast("string")
    if df.filter(invert(date_text.rlike(r"^\d{4}-\d{2}-\d{2}$"))).count() > 0:
        raise ValueError("Invalid date format")

    parsed_timestamp = F.try_to_timestamp(date_text, F.lit("yyyy-MM-dd"))
    if df.filter(parsed_timestamp.isNull()).count() > 0:
        raise ValueError("Invalid date value")
    parsed_date = F.to_date(parsed_timestamp)

    derived_checks = [
        ("date_key", F.date_format(parsed_date, "yyyyMMdd").cast("int")),
        ("year", F.year(parsed_date)),
        ("month", F.month(parsed_date)),
        ("dow", F.dayofweek(parsed_date)),
        ("is_black_friday", parsed_date == F.to_date(F.lit("2017-11-24"))),
        (
            "event_window",
            parsed_date.between(
                F.to_date(F.lit("2017-11-24")), F.to_date(F.lit("2017-11-26"))
            ),
        ),
        ("is_weekend", F.dayofweek(parsed_date).isin(1, 7)),
    ]
    for column_name, expected in derived_checks:
        inconsistent = F.col(column_name) != expected
        if df.filter(inconsistent).count() > 0:
            raise ValueError(f"Inconsistent {column_name} for calendar_date")

    return df


validate_date_gold = validate_dim_date_gold
