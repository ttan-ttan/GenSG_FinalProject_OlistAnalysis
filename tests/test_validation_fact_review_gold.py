""" TESTS for validation_fact_review_gold.py """

import pytest

from src.validation_fact_review_gold import validate_fact_review_gold


def make_valid_df(spark):
    """Create a valid DataFrame for testing validate_fact_review_gold."""
    return spark.createDataFrame(
        [("r1", "o1", 20180101, 5), ("r2", "o2", 20180102, 3)],
        ["review_id", "order_id", "order_date_key", "review_score"],
    )


def test_validate_fact_review_gold_accepts_valid_data(spark):
    """Test that validate_fact_review_gold accepts a valid DataFrame without errors."""
    out = validate_fact_review_gold(make_valid_df(spark))
    assert out.count() == 2


def test_validate_fact_review_gold_rejects_invalid_score(spark):
    """Test that validate_fact_review_gold raises an error for invalid review_score values."""
    df = make_valid_df(spark)
    df = df.withColumn("review_score", df["review_score"].cast("int"))
    bad = df.withColumn("review_score", df["review_score"] + 10)
    with pytest.raises(ValueError):
        validate_fact_review_gold(bad)


def test_validate_fact_review_gold_rejects_duplicate_review_id(spark):
    """review_id is the declared primary key."""
    df = make_valid_df(spark).union(
        spark.createDataFrame(
            [("r1", "o3", 20180103, 4)],
            ["review_id", "order_id", "order_date_key", "review_score"],
        )
    )
    with pytest.raises(ValueError):
        validate_fact_review_gold(df)
