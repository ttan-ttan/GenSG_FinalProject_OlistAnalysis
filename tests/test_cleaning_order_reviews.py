"""
test_cleaning_order_reviews.py

Each function starting with "test_" is one independent test case.
pytest runs every one of them and tells you which passed/failed.

The pattern in each test is usually:
  1. build a tiny fake DataFrame that represents one specific scenario
  2. run clean_order_reviews() on it
  3. assert (check) that the output looks the way we expect
"""

from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import types as T

from src.cleaning_reviews import CLEANED_COLUMNS, SchemaError, clean_order_reviews

# this describes what a RAW row looks like before cleaning - everything
# is a string, just like it would be coming straight out of a CSV
RAW_SCHEMA = T.StructType(
    [
        T.StructField("review_id", T.StringType(), True),
        T.StructField("order_id", T.StringType(), True),
        T.StructField("review_score", T.StringType(), True),
        T.StructField("review_comment_title", T.StringType(), True),
        T.StructField("review_comment_message", T.StringType(), True),
        T.StructField("review_creation_date", T.StringType(), True),
        T.StructField("review_answer_timestamp", T.StringType(), True),
    ]
)


def _row(**overrides):
    """Helper: builds one fake row with sensible defaults, letting each
    test override just the fields it actually cares about. Saves us from
    retyping all 7 fields in every single test."""
    base = {
        "review_id": "r1",
        "order_id": "o1",
        "review_score": "5",
        "review_comment_title": None,
        "review_comment_message": None,
        "review_creation_date": "2018-01-18 00:00:00",
        "review_answer_timestamp": "2018-01-18 21:46:59",
    }
    base.update(overrides)
    return Row(**base)


def test_raises_on_missing_columns(spark):
    # this fake DataFrame is missing most of the required columns on purpose
    df = spark.createDataFrame([Row(review_id="r1", order_id="o1")])
    try:
        clean_order_reviews(df)
        # if we get here, no error was raised - which is wrong, so fail the test
        assert False, "expected SchemaError"
    except SchemaError:
        pass  # this is the expected outcome


def test_casts_types_and_parses_timestamps(spark):
    df = spark.createDataFrame([_row()], schema=RAW_SCHEMA)
    row = clean_order_reviews(df).collect()[
        0
    ]  # collect() pulls the data back to Python so we can inspect it

    # review_score should now be a real Python int, not a string
    assert isinstance(row["review_score"], int)
    assert row["review_score"] == 5
    # timestamps should now be real datetime objects
    assert row["review_creation_date"] == datetime(
        2018, 1, 18, 0, 0, 0, tzinfo=timezone.utc
    )
    assert row["review_answer_timestamp"] == datetime(
        2018, 1, 18, 21, 46, 59
    )  # noqa: DTZ001


def test_blank_strings_become_null_and_flags_are_derived(spark):
    df = spark.createDataFrame(
        [
            _row(
                review_id="r2",
                review_comment_title="",
                review_comment_message="Great product!",
            )
        ],
        schema=RAW_SCHEMA,
    )
    row = clean_order_reviews(df).collect()[0]

    assert row["review_comment_title"] is None  # "" should have become null
    assert row["has_title"] is False  # no title -> has_title should be False
    assert row["has_message"] is True  # has a message -> has_message should be True
    assert row["message_length"] == len("Great product!")


def test_drops_rows_with_invalid_score(spark):
    df = spark.createDataFrame(
        [
            _row(review_id="r_bad", review_score="9"),
            _row(review_id="r_good", review_score="4"),
        ],
        schema=RAW_SCHEMA,
    )
    # only r_good should survive, since score 9 is outside 1-5
    ids = {r["review_id"] for r in clean_order_reviews(df).collect()}
    assert ids == {"r_good"}


def test_drops_rows_with_unparseable_score(spark):
    df = spark.createDataFrame(
        [
            _row(review_id="r_bad", review_score="not_a_number"),
            _row(review_id="r_good", review_score="3"),
        ],
        schema=RAW_SCHEMA,
    )
    # "not_a_number" can't be cast to int, so it becomes null and gets dropped
    ids = {r["review_id"] for r in clean_order_reviews(df).collect()}
    assert ids == {"r_good"}


def test_drops_rows_with_null_ids(spark):
    df = spark.createDataFrame(
        [
            _row(review_id=None),
            _row(order_id=None, review_id="r_keep"),
            _row(review_id="r_good2", order_id="o_good2"),
        ],
        schema=RAW_SCHEMA,
    )
    # both rows with a missing id should be dropped, only the fully-filled one remains
    ids = {r["review_id"] for r in clean_order_reviews(df).collect()}
    assert ids == {"r_good2"}


def test_deduplicates_on_review_id_keeping_latest_answer(spark):
    # same review_id "r1" appears twice, once answered earlier and once later
    df = spark.createDataFrame(
        [
            _row(
                review_id="r1",
                order_id="o1",
                review_answer_timestamp="2018-01-18 10:00:00",
            ),
            _row(
                review_id="r1",
                order_id="o2",
                review_answer_timestamp="2018-01-20 10:00:00",
            ),
        ],
        schema=RAW_SCHEMA,
    )
    rows = clean_order_reviews(df).collect()
    assert len(rows) == 1  # only one row should remain for r1
    assert (
        rows[0]["order_id"] == "o2"
    )  # it should be the one with the LATER answer time


def test_drops_exact_duplicate_rows(spark):
    # two completely identical rows
    df = spark.createDataFrame([_row(), _row()], schema=RAW_SCHEMA)
    assert clean_order_reviews(df).count() == 1  # should collapse down to just 1


def test_response_time_hours_computed(spark):
    df = spark.createDataFrame(
        [
            _row(
                review_creation_date="2018-01-18 00:00:00",
                review_answer_timestamp="2018-01-19 00:00:00",  # exactly 24 hours later
            )
        ],
        schema=RAW_SCHEMA,
    )
    row = clean_order_reviews(df).collect()[0]
    assert row["response_time_hours"] == 24.0


def test_output_columns_match_expected(spark):
    # make sure the final table always has the exact columns we promised,
    # in the exact order we promised
    df = spark.createDataFrame([_row()], schema=RAW_SCHEMA)
    assert clean_order_reviews(df).columns == CLEANED_COLUMNS
