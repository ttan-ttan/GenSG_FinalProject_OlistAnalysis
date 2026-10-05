"""
validation_fact_review_gold
Validation for the Gold fact_review table.
"""

from __future__ import annotations
from operator import invert
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "review_id",
    "order_id",
    "order_date_key",
    "review_score",
]


def validate_fact_review_gold(
    df: DataFrame, fact_orders: DataFrame | None = None
) -> DataFrame:
    """Validate Gold review rows."""
    # Schema check
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns in gold_fact_review: {missing}")

    # Null checks
    for col in REQUIRED_COLUMNS:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in gold_fact_review: {col}")

    # Review score validation
    if df.filter(invert(F.col("review_score").between(1, 5))).count() > 0:
        raise ValueError("Invalid review_score detected in gold_fact_review")

    # Duplicate review_id
    dup = df.groupBy("review_id").count().filter(F.col("count") > 1)
    if dup.count() > 0:
        raise ValueError("Duplicate review_id detected in gold_fact_review")

    # FK orphan check
    if fact_orders is not None:
        orders = fact_orders.select(
            "order_id", F.col("date_key").alias("order_date_key")
        )
        orphans = df.join(orders, ["order_id", "order_date_key"], "left_anti")
        if orphans.limit(1).count() > 0:
            raise ValueError(
                "Orphan FK detected in gold_fact_review (order_id/date_key mismatch)")

    return df


validate_review_gold = validate_fact_review_gold
