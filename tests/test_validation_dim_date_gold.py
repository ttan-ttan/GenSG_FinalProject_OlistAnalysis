import pytest

from src.validation_dim_date_gold import validate_dim_date_gold


def make_valid_dim(spark):
    return spark.createDataFrame(
        [
            ("2018-01-01", 2018, 1, "January", 1, 1, False),
            ("2018-01-02", 2018, 1, "January", 2, 2, False),
        ],
        [
            "calendar_date",
            "year_num",
            "month_num",
            "month_name",
            "day_num",
            "day_of_week_num",
            "is_weekend",
        ],
    )


def test_valid_dim_date_gold_passes(spark):
    out = validate_dim_date_gold(make_valid_dim(spark))
    assert out.count() == 2


def test_dim_date_gold_rejects_duplicate_calendar_date(spark):
    df = make_valid_dim(spark).union(
        spark.createDataFrame(
            [("2018-01-01", 2018, 1, "January", 1, 1, False)],
            [
                "calendar_date",
                "year_num",
                "month_num",
                "month_name",
                "day_num",
                "day_of_week_num",
                "is_weekend",
            ],
        )
    )
    with pytest.raises(ValueError):
        validate_dim_date_gold(df)


def test_dim_date_gold_rejects_null_or_invalid_date(spark):
    df = spark.createDataFrame(
        [(None, 2018, 1, "January", 1, 1, False)],
        [
            "calendar_date",
            "year_num",
            "month_num",
            "month_name",
            "day_num",
            "day_of_week_num",
            "is_weekend",
        ],
    )
    with pytest.raises(ValueError):
        validate_dim_date_gold(df)
