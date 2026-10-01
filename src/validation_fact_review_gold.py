from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp",
]


def validate_fact_review_gold(df: DataFrame) -> DataFrame:
    """Validate Gold review rows."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    for col in ["review_id", "order_id", "review_score", "review_creation_date", "review_answer_timestamp"]:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in Gold fact review: {col}")

    if df.filter(F.col("review_score").between(1, 5).__invert__()).count() > 0:
        raise ValueError("Invalid review_score detected")

    dup = df.groupBy("review_id", "order_id").count().filter(
        F.col("count") > 1)
    if dup.count() > 0:
        raise ValueError("Duplicate review_id/order_id pair detected")

    if df.filter(F.col("review_answer_timestamp") < F.col("review_creation_date")).count() > 0:
        raise ValueError("review_answer_timestamp before review_creation_date")

    return df


validate_review_gold = validate_fact_review_gold
