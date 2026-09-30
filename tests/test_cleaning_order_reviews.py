# test_cleaning_order_reviews.py
# Unit tests for cleaning_reviews.py. Run with: pytest

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.cleaning_reviews import (
    load_reviews, standardise_reviews, drop_missing_key_fields, keep_valid_scores,
    keep_valid_dates, remove_duplicates, fill_missing_comments,
    latest_review_per_order, clean_reviews,
)

COLUMNS = [
    "review_id", "order_id", "review_score", "review_comment_title",
    "review_comment_message", "review_creation_date", "review_answer_timestamp",
]
RAW_SCHEMA = StructType([StructField(column, StringType(), True) for column in COLUMNS])


@pytest.fixture(scope="session")
def spark():
    """One small local Spark session shared by all tests."""
    return (
        SparkSession.builder.master("local[1]").appName("reviews_tests")
        .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def make_df(spark, rows):
    """Build a test DataFrame of text values, like a freshly loaded CSV."""
    return spark.createDataFrame(rows, RAW_SCHEMA)


def good_row(review_id="r1", order_id="o1", score="5", title=None, msg=None,
             created="2018-01-18 00:00:00", answered="2018-01-18 21:46:59"):
    """A valid review row we can tweak in each test."""
    return (review_id, order_id, score, title, msg, created, answered)


def test_load_reviews_handles_line_breaks(spark, tmp_path):
    # A comment with a line break inside quotes must stay in ONE row
    csv_file = tmp_path / "reviews.csv"
    csv_file.write_text(
        '"review_id","order_id","review_score","review_comment_title","review_comment_message",'
        '"review_creation_date","review_answer_timestamp"\n'
        '"r1","o1",5,"","Muito bom\nchegou rapido","2018-01-18 00:00:00","2018-01-18 21:46:59"\n',
        encoding="utf-8",
    )
    df = load_reviews(spark, str(csv_file))
    assert df.count() == 1


def test_standardise_types_and_blank_to_null(spark):
    df = standardise_reviews(make_df(spark, [good_row(title="  ", msg="Bom\nproduto")]))
    row = df.collect()[0]
    assert dict(df.dtypes)["review_creation_date"] == "timestamp"
    assert dict(df.dtypes)["review_score"] == "int"
    assert row["review_comment_title"] is None          # blank became null
    assert row["review_comment_message"] == "Bom produto"  # line break removed


def test_missing_key_fields_dropped(spark):
    df = standardise_reviews(make_df(spark, [good_row(), good_row(review_id="r2", answered="not a date")]))
    assert drop_missing_key_fields(df).count() == 1


def test_invalid_scores_removed(spark):
    df = standardise_reviews(make_df(spark, [good_row(score="0"), good_row(review_id="r2", score="6"), good_row(review_id="r3", score="3")]))
    assert keep_valid_scores(df).count() == 1


def test_answer_before_creation_removed(spark):
    df = standardise_reviews(make_df(spark, [good_row(created="2018-01-20 00:00:00", answered="2018-01-19 10:00:00")]))
    assert keep_valid_dates(df).count() == 0


def test_duplicates_keep_latest_answer(spark):
    rows = [good_row(answered="2018-01-18 10:00:00"), good_row(answered="2018-01-19 10:00:00")]
    out = remove_duplicates(standardise_reviews(make_df(spark, rows)))
    assert out.count() == 1
    assert str(out.collect()[0]["review_answer_timestamp"]).startswith("2018-01-19")


def test_same_review_id_on_different_orders_is_kept(spark):
    rows = [good_row(order_id="o1"), good_row(order_id="o2")]
    assert remove_duplicates(standardise_reviews(make_df(spark, rows))).count() == 2


def test_fill_missing_comments_adds_flags(spark):
    df = fill_missing_comments(standardise_reviews(make_df(spark, [good_row(title=None, msg="Otimo")])))
    row = df.collect()[0]
    assert row["review_comment_title"] == "No title"
    assert row["review_comment_message"] == "Otimo"
    assert row["has_comment_title"] is False
    assert row["has_comment_message"] is True


def test_latest_review_per_order(spark):
    rows = [
        good_row(review_id="r1", order_id="o1", answered="2018-01-18 10:00:00"),
        good_row(review_id="r2", order_id="o1", answered="2018-01-19 10:00:00"),
        good_row(review_id="r3", order_id="o2"),
    ]
    out = latest_review_per_order(standardise_reviews(make_df(spark, rows)))
    assert out.count() == 2
    assert out.filter("order_id = 'o1'").collect()[0]["review_id"] == "r2"


def test_full_pipeline(spark):
    rows = [
        good_row(review_id="r1", msg="Otimo"),                        # good row
        good_row(review_id="r1", msg="Otimo"),                        # exact duplicate
        good_row(review_id="r2", order_id="o2", score="9"),           # bad score -> dropped
        good_row(review_id="r3", order_id="o3", title=""),            # blank title -> placeholder
    ]
    out = clean_reviews(make_df(spark, rows))
    assert out.count() == 2
    assert out.filter("review_comment_title IS NULL OR review_comment_message IS NULL").count() == 0


def test_full_pipeline_one_review_per_order(spark):
    rows = [
        good_row(review_id="r1", order_id="o1", answered="2018-01-18 10:00:00"),
        good_row(review_id="r2", order_id="o1", answered="2018-01-19 10:00:00"),
    ]
    assert clean_reviews(make_df(spark, rows), one_review_per_order=True).count() == 1