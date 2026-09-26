"""
test_validation_reviews.py

Same idea as the cleaning tests: each test builds a tiny fake DataFrame
for one scenario, runs validate_order_reviews() (or quarantine_invalid_rows()),
and checks the result matches what we expect.
"""

from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import types as T

from src.validation_reviews import quarantine_invalid_rows, validate_order_reviews

# for validation tests we build data that already has the RIGHT types
# (unlike the cleaning tests, where everything starts as text)
SCHEMA = T.StructType(
    [
        T.StructField("review_id", T.StringType(), True),
        T.StructField("order_id", T.StringType(), True),
        T.StructField("review_score", T.IntegerType(), True),
        T.StructField("review_creation_date", T.TimestampType(), True),
        T.StructField("review_answer_timestamp", T.TimestampType(), True),
    ]
)


def _row(**overrides):
    """Same helper idea as in the cleaning tests: sensible defaults,
    override only what the test cares about."""
    base = dict(
        review_id="r1",
        order_id="o1",
        review_score=5,
        review_creation_date=datetime(2018, 1, 18, 0, 0, 0),
        review_answer_timestamp=datetime(2018, 1, 18, 21, 46, 59),
    )
    base.update(overrides)
    return Row(**base)


def test_missing_columns_fails_immediately(spark):
    df = spark.createDataFrame([Row(review_id="r1", order_id="o1")])
    result = validate_order_reviews(df)
    assert result.passed is False
    assert any("missing required column" in e for e in result.errors)


def test_clean_data_passes(spark):
    # two perfectly valid rows -> should pass with zero errors
    df = spark.createDataFrame([_row(), _row(review_id="r2", order_id="o2")], schema=SCHEMA)
    result = validate_order_reviews(df)
    assert result.passed is True
    assert result.errors == []
    assert result.row_count == 2


def test_null_ids_fail(spark):
    df = spark.createDataFrame([_row(review_id=None)], schema=SCHEMA)
    result = validate_order_reviews(df)
    assert result.passed is False
    assert result.metrics["null_review_id"] == 1


def test_out_of_range_score_fails(spark):
    df = spark.createDataFrame([_row(review_score=9)], schema=SCHEMA)  # 9 is outside 1-5
    result = validate_order_reviews(df)
    assert result.passed is False
    assert result.metrics["review_score_out_of_range"] == 1


def test_reversed_timestamps_fail(spark):
    # answer timestamp is BEFORE creation timestamp - shouldn't be possible
    df = spark.createDataFrame(
        [
            _row(
                review_creation_date=datetime(2018, 1, 20, 0, 0, 0),
                review_answer_timestamp=datetime(2018, 1, 18, 0, 0, 0),
            )
        ],
        schema=SCHEMA,
    )
    result = validate_order_reviews(df)
    assert result.passed is False
    assert result.metrics["review_answer_before_creation"] == 1


def test_duplicate_review_id_is_warning_not_error(spark):
    # same review_id "r1" used twice - this should be a WARNING, not a failure
    df = spark.createDataFrame([_row(), _row(order_id="o2")], schema=SCHEMA)
    result = validate_order_reviews(df)
    assert result.passed is True   # still passes!
    assert result.metrics["duplicate_review_id_groups"] == 1
    assert any("appear more than once" in w for w in result.warnings)


def test_raise_if_failed_raises_valueerror(spark):
    df = spark.createDataFrame([_row(review_score=None)], schema=SCHEMA)
    result = validate_order_reviews(df)
    try:
        result.raise_if_failed()
        assert False, "expected ValueError"
    except ValueError:
        pass  # this is what we wanted to happen


def test_quarantine_invalid_rows_tags_reasons(spark):
    df = spark.createDataFrame(
        [
            _row(review_id="bad1", review_score=None),   # broken: missing score
            _row(review_id="bad2", order_id=None),        # broken: missing order_id
            _row(review_id="good", review_score=4),       # this one is fine
        ],
        schema=SCHEMA,
    )
    quarantined = quarantine_invalid_rows(df).collect()

    # build a lookup of review_id -> list of reasons it was flagged
    reasons_by_id = {}
    for r in quarantined:
        reasons_by_id.setdefault(r["review_id"], []).append(r["validation_reason"])

    assert "good" not in reasons_by_id                          # the good row shouldn't show up at all
    assert "invalid review_score" in reasons_by_id["bad1"]
    assert "null order_id" in reasons_by_id["bad2"]