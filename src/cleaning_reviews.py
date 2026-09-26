"""
cleaning_reviews.py

This file takes the RAW order_reviews table (straight from the CSV) and
turns it into a CLEAN table that's safe to use in analysis.

"Cleaning" here means:
    - making sure the right columns are all there
    - fixing data types (e.g. text "5" -> actual number 5)
    - turning weird blank text into real nulls
    - removing rows that are broken beyond repair
    - removing duplicate rows
    - adding a few extra helper columns that are handy later
"""

from __future__ import annotations

import logging
from typing import Final

from pyspark.sql import DataFrame, Window       # DataFrame = the table object, Window = used for "per group" logic
from pyspark.sql import functions as F           # F = shortcut for Spark's built-in column functions
from pyspark.sql import types as T               # T = shortcut for Spark's data types (Integer, Timestamp, etc.)

# logger lets us print progress/info messages instead of using print()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# These are just constants (fixed values) so we don't hardcode strings
# everywhere in the code below. If a column name ever changes, we only
# need to change it here.
# ---------------------------------------------------------------------------

# The columns we EXPECT to receive from the raw CSV/table
RAW_COLUMNS: Final[list[str]] = [
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp",
]

# The date format used in the CSV, e.g. "2018-01-18 00:00:00"
# yyyy = year, MM = month, dd = day, HH:mm:ss = hour:minute:second
TIMESTAMP_FORMAT: Final[str] = "yyyy-MM-dd HH:mm:ss"

# The columns we OUTPUT after cleaning (raw columns + a few new ones we add)
CLEANED_COLUMNS: Final[list[str]] = [
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp",
    "has_title",             # True/False - did this review have a title?
    "has_message",           # True/False - did this review have a message?
    "message_length",        # how many characters long the message is
    "response_time_hours",   # how many hours between review being written and answered
]


class SchemaError(Exception):
    """Custom error we raise if the table is missing columns we need."""


def _assert_required_columns(df: DataFrame) -> None:
    """Check that every column we need is actually present. If not, stop early
    with a clear error instead of failing later with a confusing message."""
    missing = [c for c in RAW_COLUMNS if c not in df.columns]
    if missing:
        raise SchemaError(f"order_reviews is missing required column(s): {missing}")


def _trim_strings(df: DataFrame, columns: list[str]) -> DataFrame:
    """Remove leading/trailing spaces from text columns.
    e.g. "  hello " -> "hello" """
    for c in columns:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df


def _blank_to_null(df: DataFrame, columns: list[str]) -> DataFrame:
    """Turn empty string "" into a real null/None value.
    CSVs often store missing values as "" instead of a true null, so we
    fix that here so later null-checks actually work."""
    for c in columns:
        df = df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))
    return df


def clean_order_reviews(df: DataFrame) -> DataFrame:
    """
    Main cleaning function. Takes the raw DataFrame in, returns a clean
    DataFrame out. Each numbered step below does one specific job.
    """
    # Step 0: fail fast if the table doesn't have the columns we expect
    _assert_required_columns(df)

    # count() triggers Spark to actually scan the data and tell us how
    # many rows we started with (useful for logging later)
    start_count = df.count()

    # Step 1: trim whitespace off text columns
    df = _trim_strings(
        df,
        ["review_id", "order_id", "review_comment_title", "review_comment_message"],
    )

    # Step 2: convert "" (empty string) into actual null for the optional
    # comment fields, and for the ID fields too
    df = _blank_to_null(df, ["review_comment_title", "review_comment_message"])
    df = _blank_to_null(df, ["review_id", "order_id"])

    # Step 3: review_score comes in as TEXT from the CSV (e.g. "5").
    # .cast(IntegerType()) converts it to a real number.
    # If a value can't be converted (like "abc"), Spark turns it into null
    # automatically, and we remove those null rows in Step 5.
    df = df.withColumn("review_score", F.col("review_score").cast(T.IntegerType()))

    # Step 4: convert the date TEXT columns into real Spark TIMESTAMP type,
    # using the format we defined above. Again, anything that doesn't match
    # the expected format becomes null, and gets removed in Step 5.
    df = df.withColumn(
        "review_creation_date",
        F.to_timestamp(F.col("review_creation_date"), TIMESTAMP_FORMAT),
    ).withColumn(
        "review_answer_timestamp",
        F.to_timestamp(F.col("review_answer_timestamp"), TIMESTAMP_FORMAT),
    )

    # Step 5: keep ONLY rows that meet all our "must-have" rules:
    #   - review_id is not missing
    #   - order_id is not missing
    #   - review_score exists AND is between 1 and 5
    #   - both timestamps were parsed successfully
    # Rows that fail any of these get dropped, because we can't trust them.
    df = df.filter(
        F.col("review_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("review_score").isNotNull()
        & F.col("review_score").between(1, 5)
        & F.col("review_creation_date").isNotNull()
        & F.col("review_answer_timestamp").isNotNull()
    )
    after_hard_filter_count = df.count()  # how many rows survived Step 5

    # Step 6: remove exact duplicate rows (every single column identical)
    df = df.dropDuplicates()

    # Step 7: the dataset has some review_id values that repeat (same
    # review_id used for more than one order). We only want ONE row per
    # review_id, so we keep the one with the most recent answer time.
    #
    # Window + row_number() is Spark's way of saying:
    # "group rows by review_id, sort each group by answer time (newest
    # first), and number them 1, 2, 3... within the group"
    window = Window.partitionBy("review_id").orderBy(F.col("review_answer_timestamp").desc())
    df = (
        df.withColumn("_rn", F.row_number().over(window))  # add a row-number column
        .filter(F.col("_rn") == 1)                          # keep only the #1 (newest) row per review_id
        .drop("_rn")                                        # remove the helper column, we don't need it anymore
    )

    # Step 8: add a few extra columns that make later analysis easier
    df = (
        df.withColumn("has_title", F.col("review_comment_title").isNotNull())
        .withColumn("has_message", F.col("review_comment_message").isNotNull())
        .withColumn(
            "message_length",
            # F.length() counts characters; if message is null, count as 0
            F.coalesce(F.length(F.col("review_comment_message")), F.lit(0)),
        )
        .withColumn(
            "response_time_hours",
            # cast timestamps to "long" (seconds since 1970), subtract them
            # to get seconds difference, then divide by 3600 to get hours
            (
                F.col("review_answer_timestamp").cast("long")
                - F.col("review_creation_date").cast("long")
            )
            / 3600.0,
        )
    )

    # Step 9: pick only the columns we want, in a fixed order, and sort
    # by review_id so the output is predictable/easy to compare
    df = df.select(*CLEANED_COLUMNS).orderBy("review_id")

    final_count = df.count()

    # log a summary so whoever runs this can see how many rows were
    # dropped and why, without having to read through the code
    logger.info(
        "clean_order_reviews: start=%d after_hard_filter=%d final=%d "
        "(dropped_invalid=%d, dropped_duplicate_review_id=%d)",
        start_count,
        after_hard_filter_count,
        final_count,
        start_count - after_hard_filter_count,
        after_hard_filter_count - final_count,
    )

    return df