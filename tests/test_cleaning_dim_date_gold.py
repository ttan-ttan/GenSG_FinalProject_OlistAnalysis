"""Tests for the Gold date dimension builder."""

from src.cleaning_dim_date_gold import build_dim_date_gold


def test_build_dim_date_gold_creates_one_row_per_purchase_date(spark):
    orders = spark.createDataFrame(
        [
            ("2018-01-01 10:00:00",),
            ("2018-01-01 16:30:00",),
            ("2018-01-03 12:00:00",),
        ],
        ["order_purchase_timestamp"],
    )

    result = build_dim_date_gold(orders)

    assert result.count() == 2
    assert result.columns == [
        "date_key",
        "date",
        "year",
        "month",
        "day",
        "weekday",
    ]
    jan_first = result.filter("date = '2018-01-01'").first()
    assert jan_first["date_key"] == "20180101"
    assert jan_first["year"] == 2018
    assert jan_first["month"] == 1
    assert jan_first["day"] == 1
    assert jan_first["weekday"] == "Monday"


def test_build_dim_date_gold_keeps_null_purchase_date(spark):
    orders = spark.createDataFrame(
        [(None,), ("2018-01-02 12:00:00",)],
        ["order_purchase_timestamp"],
    )

    result = build_dim_date_gold(orders)

    assert result.count() == 2
    assert result.filter("date is null").first()["date_key"] is None
