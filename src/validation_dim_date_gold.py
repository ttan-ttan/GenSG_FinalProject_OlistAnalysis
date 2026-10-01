""" validation_dim_date_gold
This module contains functions to validate the Gold date dimension table."""
from __future__ import annotations

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

    if df.filter(F.col("calendar_date").isNull()).count() > 0:
        raise ValueError("calendar_date contains null values")

    dup = df.groupBy("calendar_date").count().filter(F.col("count") > 1)
    if dup.count() > 0:
        raise ValueError("Duplicate calendar_date values detected")

    for col in ["year_num", "month_num", "day_num", "day_of_week_num"]:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(
                f"Null critical field in Gold date dimension: {col}")

    if (
        df.filter(
            ~F.col("calendar_date").cast(
                "string").rlike(r"^\d{4}-\d{2}-\d{2}$")
        ).count()
        > 0
    ):
        raise ValueError("Invalid calendar_date format")

    return df


validate_date_gold = validate_dim_date_gold
