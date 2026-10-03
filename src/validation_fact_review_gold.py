""" validation_fact_review_gold
This module contains functions to validate the Gold fact review table."""

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
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    for col in REQUIRED_COLUMNS:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in Gold fact review: {col}")

    if df.filter(invert(F.col("review_score").between(1, 5))).count() > 0:
        raise ValueError("Invalid review_score detected")

    dup = df.groupBy("review_id").count().filter(F.col("count") > 1)
    if dup.count() > 0:
        raise ValueError("Duplicate review_id detected")

    if fact_orders is not None:
        orders = fact_orders.select(
            "order_id", F.col("date_key").alias("order_date_key")
        )
        if (
            df.join(orders, ["order_id", "order_date_key"], "left_anti")
            .limit(1)
            .count()
        ):
            raise ValueError("Review order_date_key does not match its fact_order")

    return df


validate_review_gold = validate_fact_review_gold
