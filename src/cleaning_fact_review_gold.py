""" cleaning_fact_review_gold
This module contains functions to clean the Gold fact review table."""

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


def clean_fact_review_gold(df: DataFrame) -> DataFrame:
    """Clean the Gold fact review table."""
    df = df.select(*[c.strip().lower() if c in df.columns else c for c in df.columns])
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = (
        df.withColumn("review_id", F.trim(F.col("review_id").cast("string")))
        .withColumn("order_id", F.trim(F.col("order_id").cast("string")))
        .withColumn("review_score", F.col("review_score").cast("int"))
        .withColumn(
            "review_comment_title",
            F.when(F.col("review_comment_title") == "", None).otherwise(
                F.trim(F.col("review_comment_title"))
            ),
        )
        .withColumn(
            "review_comment_message",
            F.when(F.col("review_comment_message") == "", None).otherwise(
                F.trim(F.col("review_comment_message"))
            ),
        )
        .withColumn(
            "review_creation_date", F.to_timestamp(F.col("review_creation_date"))
        )
        .withColumn(
            "review_answer_timestamp", F.to_timestamp(F.col("review_answer_timestamp"))
        )
    )

    df = (
        df.filter(F.col("review_id").isNotNull())
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("review_score").between(1, 5))
        .filter(F.col("review_creation_date").isNotNull())
        .filter(F.col("review_answer_timestamp").isNotNull())
        .filter(F.col("review_answer_timestamp") >= F.col("review_creation_date"))
    )

    df = df.dropDuplicates()
    return df.select(*REQUIRED_COLUMNS)


clean_review_gold = clean_fact_review_gold
