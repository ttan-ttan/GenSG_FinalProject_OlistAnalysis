""" TESTS for validation_fact_review_gold.py """
import pytest

from src.validation_fact_review_gold import validate_fact_review_gold


def make_valid_df(spark):
    """ Create a valid DataFrame for testing validate_fact_review_gold."""
    return spark.createDataFrame(
        [
            (
                "r1",
                "o1",
                5,
                "Good",
                "Nice order",
                "2018-01-18 00:00:00",
                "2018-01-18 21:46:59",
            ),
            (
                "r2",
                "o2",
                3,
                "Okay",
                "Average",
                "2018-01-19 00:00:00",
                "2018-01-19 04:00:00",
            ),
        ],
        [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ],
    )


def test_validate_fact_review_gold_accepts_valid_data(spark):
    """ Test that validate_fact_review_gold accepts a valid DataFrame without errors."""
    out = validate_fact_review_gold(make_valid_df(spark))
    assert out.count() == 2


def test_validate_fact_review_gold_rejects_invalid_score(spark):
    """ Test that validate_fact_review_gold raises an error for invalid review_score values."""
    df = make_valid_df(spark)
    df = df.withColumn("review_score", df["review_score"].cast("int"))
    bad = df.withColumn("review_score", df["review_score"] + 10)
    with pytest.raises(ValueError):
        validate_fact_review_gold(bad)


def test_validate_fact_review_gold_rejects_duplicate_review_order_key(spark):
    """ Test that validate_fact_review_gold raises an error for duplicate review_id 
    and order_id combinations."""
    df = make_valid_df(spark).union(
        spark.createDataFrame(
            [
                (
                    "r1",
                    "o1",
                    5,
                    "Good",
                    "Nice order",
                    "2018-01-18 00:00:00",
                    "2018-01-18 21:46:59",
                )
            ],
            [
                "review_id",
                "order_id",
                "review_score",
                "review_comment_title",
                "review_comment_message",
                "review_creation_date",
                "review_answer_timestamp",
            ],
        )
    )
    with pytest.raises(ValueError):
        validate_fact_review_gold(df)
