""" test_cleaning_dim_date_gold  """

from src.cleaning_dim_date_gold import build_dim_date, clean_dim_date_gold


def test_build_dim_date_creates_calendar_rows(spark):
    """Build one keyed row per calendar day across order-date roles."""
    df = spark.createDataFrame(
        [
            ("2018-01-01 10:00:00", "2018-01-03 12:00:00"),
            ("2018-01-03 12:00:00", None),
        ],
        ["order_purchase_timestamp", "order_delivered_customer_date"],
    )
    out = build_dim_date(df, "order_purchase_timestamp")
    assert out.count() == 3
    assert out.filter("date = '2018-01-01'").first()["date_key"] == 20180101
    assert out.columns == [
        "date_key",
        "date",
        "year",
        "month",
        "dow",
        "is_black_friday",
        "event_window",
        "is_weekend",
    ]


def test_build_dim_date_includes_each_order_date_role(spark):
    df = spark.createDataFrame(
        [("2017-11-24 10:00:00", "2017-11-25 10:00:00")],
        ["order_purchase_timestamp", "order_approved_at"],
    )
    dates = build_dim_date(df)
    event_day = dates.filter("date = '2017-11-24'").first()
    assert event_day["is_black_friday"] is True
    assert event_day["event_window"] is True
    assert dates.filter("date = '2017-11-25'").first()["is_weekend"] is True


def test_clean_dim_date_gold_standardizes_and_removes_nulls(spark):
    """Test that clean_dim_date_gold standardizes the date format and removes nulls."""
    df = spark.createDataFrame(
        [("2018-01-01 10:00:00",), (None,), ("2018-01-02 12:00:00",)],
        ["order_purchase_timestamp"],
    )
    out = clean_dim_date_gold(df, "order_purchase_timestamp")
    assert out.filter("date is null").count() == 0
    assert out.count() == 2
