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

FACT_COLUMNS = ["review_id", "order_id", "order_date_key", "review_score"]


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


def build_fact_review_gold(reviews: DataFrame, fact_orders: DataFrame) -> DataFrame:
    """Build one review row and attach the purchase date key of its order."""
    missing = [
        column
        for column in ["review_id", "order_id", "review_score"]
        if column not in reviews.columns
    ]
    if missing:
        raise ValueError(f"Missing required review columns: {missing}")
    reviews = (
        reviews.select(
            F.trim(F.col("review_id").cast("string")).alias("review_id"),
            F.trim(F.col("order_id").cast("string")).alias("order_id"),
            F.col("review_score").cast("int").alias("review_score"),
        )
        .filter(F.col("review_id").isNotNull() & (F.col("review_id") != ""))
        .filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))
        .filter(F.col("review_score").between(1, 5))
        .dropDuplicates()
    )
    duplicates = reviews.groupBy("review_id").count().filter(F.col("count") > 1)
    if duplicates.limit(1).count() > 0:
        raise ValueError("review_id must uniquely identify a review")

    orders = fact_orders.select("order_id", F.col("date_key").alias("order_date_key"))
    fact = reviews.join(orders, "order_id", "left")
    if fact.filter(F.col("order_date_key").isNull()).limit(1).count() > 0:
        raise ValueError("Review fact contains an order_id not present in fact_order")
    return fact.select(*FACT_COLUMNS)


clean_review_gold = clean_fact_review_gold


def run_clean(spark) -> DataFrame:
    """Build and write the Gold review fact using each order's purchase date."""
    reviews_df = spark.read.table("order_reviews_silver")
    gold_df = build_fact_review_gold(reviews_df, spark.read.table("fact_order"))
    gold_df.write.format("delta").mode("overwrite").saveAsTable("fact_review")
    print("Gold fact table 'fact_review' created successfully.")
    return gold_df
