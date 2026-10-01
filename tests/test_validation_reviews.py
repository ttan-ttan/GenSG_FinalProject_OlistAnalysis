# test_validation_reviews.py
# Unit tests for validation_reviews.py. Run with: pytest

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.validation_reviews import validate_reviews, all_passed, assert_valid
from src.cleaning_reviews import (
    clean_reviews,
    standardise_reviews,
    fill_missing_comments,
)

COLUMNS = [
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp",
]
RAW_SCHEMA = StructType([StructField(column, StringType(), True) for column in COLUMNS])


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("reviews_validation_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def good_row(
    review_id="r1",
    order_id="o1",
    score="5",
    title="Titulo",
    msg="Bom",
    created="2018-01-18 00:00:00",
    answered="2018-01-18 21:46:59",
):
    """A valid review row we can tweak in each test."""
    return (review_id, order_id, score, title, msg, created, answered)


def typed_df(spark, rows):
    """Apply types and comment placeholders WITHOUT the filtering steps,
    so we can feed the validator deliberately bad data."""
    return fill_missing_comments(
        standardise_reviews(spark.createDataFrame(rows, RAW_SCHEMA))
    )


def failed_names(results):
    """Names of the checks that failed."""
    return [r["check"] for r in results if not r["passed"]]


def test_clean_data_passes_all_checks(spark):
    raw = spark.createDataFrame(
        [good_row(), good_row(review_id="r2", order_id="o2", title=None, msg=None)],
        RAW_SCHEMA,
    )
    assert all_passed(validate_reviews(clean_reviews(raw)))


def test_missing_column_is_reported(spark):
    df = typed_df(spark, [good_row()]).drop("review_score")
    assert failed_names(validate_reviews(df)) == ["required_columns_present"]


def test_string_dates_fail(spark):
    df = spark.createDataFrame([good_row()], RAW_SCHEMA)  # raw strings, never converted
    failed = failed_names(validate_reviews(df))
    assert "creation_date_is_timestamp" in failed
    assert "answer_timestamp_is_timestamp" in failed


def test_bad_score_fails(spark):
    df = typed_df(spark, [good_row(score="7")])
    assert "score_between_1_and_5" in failed_names(validate_reviews(df))


def test_duplicate_key_fails(spark):
    df = typed_df(spark, [good_row(), good_row(msg="Outro")])
    assert "review_order_key_unique" in failed_names(validate_reviews(df))


def test_answer_before_creation_fails(spark):
    df = typed_df(
        spark, [good_row(created="2018-01-20 00:00:00", answered="2018-01-19 00:00:00")]
    )
    assert "answer_after_creation" in failed_names(validate_reviews(df))


def test_unfilled_comments_fail(spark):
    df = standardise_reviews(
        spark.createDataFrame([good_row(title=None, msg=None)], RAW_SCHEMA)
    )  # no placeholders
    assert "comments_filled" in failed_names(validate_reviews(df))


def test_line_breaks_fail(spark):
    df = fill_missing_comments(
        spark.createDataFrame([good_row(msg="linha1\nlinha2")], RAW_SCHEMA).withColumn(
            "review_score",
            __import__("pyspark.sql.functions", fromlist=["col"])
            .col("review_score")
            .cast("int"),
        )
    )
    # dates are still strings here, so we only check that the newline check itself fires
    assert "no_line_breaks_in_comments" in failed_names(validate_reviews(df))


def test_one_review_per_order_check(spark):
    df = typed_df(
        spark, [good_row(review_id="r1"), good_row(review_id="r2")]
    )  # both on order o1
    assert "one_review_per_order" in failed_names(
        validate_reviews(df, expect_one_review_per_order=True)
    )


def test_orders_cross_check(spark):
    df = typed_df(spark, [good_row(order_id="ghost")])
    orders = spark.createDataFrame([("o1",)], ["order_id"])
    assert "order_ids_exist_in_orders" in failed_names(
        validate_reviews(df, orders_df=orders)
    )


def test_assert_valid_raises_on_failure(spark):
    df = typed_df(spark, [good_row(score="7")])
    with pytest.raises(ValueError):
        assert_valid(validate_reviews(df))
