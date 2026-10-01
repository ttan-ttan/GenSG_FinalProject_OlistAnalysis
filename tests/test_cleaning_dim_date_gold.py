""" test_cleaning_dim_date_gold  """
from src.cleaning_dim_date_gold import build_dim_date, clean_dim_date_gold


def test_build_dim_date_creates_calendar_rows(spark):
    """ Test that build_dim_date creates the correct number of 
    calendar rows from a DataFrame with timestamps."""
    df = spark.createDataFrame(
        [("2018-01-01 10:00:00",), ("2018-01-03 12:00:00",)],
        ["order_purchase_timestamp"],
    )
    out = build_dim_date(df, "order_purchase_timestamp")
    assert out.count() == 3
    assert out.filter("calendar_date = '2018-01-01'").count() == 1
    assert out.filter("month_name = 'January'").count() >= 1


def test_clean_dim_date_gold_standardizes_and_removes_nulls(spark):
    """ Test that clean_dim_date_gold standardizes the date format and removes nulls."""
    df = spark.createDataFrame(
        [("2018-01-01 10:00:00",), (None,), ("2018-01-02 12:00:00",)],
        ["order_purchase_timestamp"],
    )
    out = clean_dim_date_gold(df, "order_purchase_timestamp")
    assert out.filter("calendar_date is null").count() == 0
    assert out.count() == 2
