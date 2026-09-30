# cleaning_reviews.py
# Cleans the olist_order_reviews_dataset using PySpark.

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Date format used by both timestamp columns in the raw file
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"


def load_reviews(spark: SparkSession, path: str) -> DataFrame:
    """Read the CSV. multiLine=True is needed because review comments contain line breaks
    inside quotes; escape='"' handles quotes inside comments."""
    return spark.read.csv(
        path, header=True, inferSchema=False, multiLine=True,
        quote='"', escape='"', encoding="UTF-8",
    )


def standardise_reviews(df: DataFrame) -> DataFrame:
    """Trim text, turn empty strings into nulls, remove line breaks, and set data types."""
    # Trim every text column and turn "" into null
    for c in ["review_id", "order_id", "review_comment_title", "review_comment_message"]:
        df = df.withColumn(c, F.trim(F.col(c)))
        df = df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))

    # Replace line breaks inside comments with a single space (keeps one review per row)
    for c in ["review_comment_title", "review_comment_message"]:
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"[\r\n]+", " "))

    # Fix data types: score -> int, dates -> timestamp
    return (
        df.withColumn("review_score", F.col("review_score").cast("int"))
        .withColumn("review_creation_date", F.to_timestamp(F.col("review_creation_date"), TS_FORMAT))
        .withColumn("review_answer_timestamp", F.to_timestamp(F.col("review_answer_timestamp"), TS_FORMAT))
    )


def drop_missing_key_fields(df: DataFrame) -> DataFrame:
    """Rows must have ids, a score and both dates. Comments are allowed to be empty."""
    return df.dropna(
        how="any",
        subset=["review_id", "order_id", "review_score", "review_creation_date", "review_answer_timestamp"],
    )


def keep_valid_scores(df: DataFrame) -> DataFrame:
    """Review scores can only be 1 to 5."""
    return df.filter(F.col("review_score").between(1, 5))


def keep_valid_dates(df: DataFrame) -> DataFrame:
    """A review cannot be answered before it was created."""
    return df.filter(F.col("review_answer_timestamp") >= F.col("review_creation_date"))


def remove_duplicates(df: DataFrame) -> DataFrame:
    """Remove exact duplicates, then keep ONE row per (review_id, order_id).
    If there are several, keep the one with the latest answer timestamp.
    Note: the same review_id can legitimately appear on different orders."""
    df = df.dropDuplicates()
    window = Window.partitionBy("review_id", "order_id").orderBy(F.col("review_answer_timestamp").desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def fill_missing_comments(df: DataFrame) -> DataFrame:
    """Keep the rows (their scores are valuable) but fill empty comments with placeholders.
    First add True/False flags so we can still tell real comments from placeholders."""
    return (
        df.withColumn("has_comment_title", F.col("review_comment_title").isNotNull())
        .withColumn("has_comment_message", F.col("review_comment_message").isNotNull())
        .fillna({"review_comment_title": "No title", "review_comment_message": "No comment"})
    )


def latest_review_per_order(df: DataFrame) -> DataFrame:
    """Some orders have more than one review. Keep only the newest per order
    so joining to the orders table does not duplicate order rows."""
    window = Window.partitionBy("order_id").orderBy(
        F.col("review_answer_timestamp").desc(), F.col("review_creation_date").desc(), F.col("review_id")
    )
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def clean_reviews(df: DataFrame, one_review_per_order: bool = False) -> DataFrame:
    """Run every cleaning step in order and return the clean DataFrame."""
    df = standardise_reviews(df)
    df = drop_missing_key_fields(df)
    df = keep_valid_scores(df)
    df = keep_valid_dates(df)
    df = remove_duplicates(df)
    if one_review_per_order:
        df = latest_review_per_order(df)
    df = fill_missing_comments(df)
    return df


if __name__ == "__main__":
    # Quick manual run: python cleaning_reviews.py
    spark = SparkSession.builder.master("local[*]").appName("clean_reviews").getOrCreate()
    input_path = Path(__file__).resolve().parents[1] / "data" / "raw" / "olist_order_reviews_dataset.csv"
    raw = load_reviews(spark, str(input_path))
    clean = clean_reviews(raw)
    print("Rows before:", raw.count(), "| Rows after:", clean.count())
    clean.printSchema()