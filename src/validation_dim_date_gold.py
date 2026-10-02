""" validation_dim_date_gold
This module contains functions to validate the Gold date dimension table."""

from __future__ import annotations

from operator import invert

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "calendar_date",
    "year_num",
    "month_num",
    "month_name",
    "day_num",
    "day_of_week_num",
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

    dup = df.groupBy("calendar_date").count().filter(F.col("count") > 1)
    if dup.count() > 0:
        raise ValueError("Duplicate calendar_date values detected")

    date_text = F.col("calendar_date").cast("string")
    if df.filter(invert(date_text.rlike(r"^\d{4}-\d{2}-\d{2}$"))).count() > 0:
        raise ValueError("Invalid calendar_date format")

    parsed_timestamp = F.try_to_timestamp(date_text, F.lit("yyyy-MM-dd"))
    if df.filter(parsed_timestamp.isNull()).count() > 0:
        raise ValueError("Invalid calendar_date value")
    parsed_date = F.to_date(parsed_timestamp)

    derived_checks = [
        ("year_num", F.year(parsed_date)),
        ("month_num", F.month(parsed_date)),
        ("month_name", F.date_format(parsed_date, "MMMM")),
        ("day_num", F.dayofmonth(parsed_date)),
        ("day_of_week_num", F.dayofweek(parsed_date)),
        ("is_weekend", F.dayofweek(parsed_date).isin(1, 7)),
    ]
    for column_name, expected in derived_checks:
        inconsistent = F.col(column_name) != expected
        if df.filter(inconsistent).count() > 0:
            raise ValueError(f"Inconsistent {column_name} for calendar_date")

    return df


validate_date_gold = validate_dim_date_gold
