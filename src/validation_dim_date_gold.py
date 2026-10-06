"""
validation_dim_date_gold
Validation for the Gold date dimension (gold_dim_date).
Checks every attribute against the date it belongs to, using the same
text labels the Power BI semantic model filters on.
"""

from operator import invert

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

try:
    from cleaning_dim_date_gold import (
        OUTPUT_COLUMNS,
        PERIOD_EVENT,
        PERIOD_POST,
        PERIOD_PRE,
        add_date_attributes,
    )
except ImportError:
    from src.cleaning_dim_date_gold import (
        OUTPUT_COLUMNS,
        PERIOD_EVENT,
        PERIOD_POST,
        PERIOD_PRE,
        add_date_attributes,
    )

REQUIRED_COLUMNS = OUTPUT_COLUMNS
CHECKED_COLUMNS = [c for c in OUTPUT_COLUMNS if c != "date"]

# Expected day counts when the full 2017 study window is in the table
EXPECTED_PERIOD_DAYS = {PERIOD_PRE: 28, PERIOD_EVENT: 3, PERIOD_POST: 28}
STUDY_START, STUDY_END = "2017-10-27", "2017-12-24"


def validate_dim_date_gold(df: DataFrame) -> DataFrame:
    """Validate the Gold date dimension."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Null checks
    for col in REQUIRED_COLUMNS:
        if df.filter(F.col(col).isNull()).limit(1).count() > 0:
            raise ValueError(f"{col} contains null values")

    # Duplicate checks
    if df.groupBy("date_key").count().filter(F.col("count") > 1).count() > 0:
        raise ValueError("Duplicate date_key values detected")

    if df.groupBy("date").count().filter(F.col("count") > 1).count() > 0:
        raise ValueError("Duplicate date values detected")

    # Format check
    date_text = F.col("date").cast("string")
    if df.filter(invert(date_text.rlike(r"^\d{4}-\d{2}-\d{2}$"))).count() > 0:
        raise ValueError("Invalid date format")

    parsed_date = F.to_date(date_text, "yyyy-MM-dd")
    if df.filter(parsed_date.isNull()).count() > 0:
        raise ValueError("Invalid date value")

    # Derived checks: rebuild every attribute from the date and compare
    expected = add_date_attributes(df.select(parsed_date.alias("date"))).select(
        F.col("date").alias("_date"),
        *[F.col(c).alias(f"_exp_{c}") for c in CHECKED_COLUMNS],
    )
    joined = df.withColumn("_date", parsed_date).join(expected, "_date", "left")
    for column_name in CHECKED_COLUMNS:
        if joined.filter(F.col(column_name) != F.col(f"_exp_{column_name}")).count():
            raise ValueError(f"Inconsistent {column_name} for calendar_date")

    # Calendar must have no gaps
    lo, hi = df.agg(F.min(parsed_date), F.max(parsed_date)).first()
    if (hi - lo).days + 1 != df.count():
        raise ValueError("Calendar has missing days")

    # Study periods must be complete when the table covers the study window
    if str(lo) <= STUDY_START and str(hi) >= STUDY_END:
        counts = {
            r["event_period"]: r["count"]
            for r in df.groupBy("event_period").count().collect()
        }
        for period, days in EXPECTED_PERIOD_DAYS.items():
            if counts.get(period) != days:
                raise ValueError(
                    f"{period} has {counts.get(period)} days, expected {days}"
                )

    return df


validate_date_gold = validate_dim_date_gold
