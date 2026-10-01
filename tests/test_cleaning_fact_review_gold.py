""" test_cleaning_fact_review_gold  """
from src.cleaning_fact_review_gold import clean_fact_review_gold


def test_clean_fact_review_gold_keeps_valid_rows(spark):
    """ Test that clean_fact_review_gold keeps only valid rows and removes duplicates."""
    df = spark.createDataFrame(
        [
            (
                "r1",
                "o1",
                5,
                "Great",
                "Very good",
                "2018-01-18 00:00:00",
                "2018-01-18 21:46:59",
            ),
            (
                "r1",
                "o1",
                5,
                "Great",
                "Very good",
                "2018-01-18 00:00:00",
                "2018-01-18 21:46:59",
            ),
            (
                "r2",
                "o2",
                6,
                "Bad score",
                "Oops",
                "2018-01-19 00:00:00",
                "2018-01-19 10:00:00",
            ),
            ("r3", "o3", 3, None, None, "2018-01-20 00:00:00", "2018-01-20 10:00:00"),
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
    out = clean_fact_review_gold(df)
    assert out.filter("review_id = 'r1'").count() == 1
    assert out.filter("review_id = 'r3'").count() == 1
    assert out.filter("review_score = 5").count() == 1
