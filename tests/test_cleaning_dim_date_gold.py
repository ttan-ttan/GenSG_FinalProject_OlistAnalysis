"""test_cleaning_dim_date_gold"""

from src.cleaning_dim_date_gold import OUTPUT_COLUMNS, build_dim_date


def row_for(dim, date):
    return dim.filter(f"date = '{date}'").first()


def test_build_dim_date_full_calendar_and_columns(spark):
    """One row per day 2016-01-01..2018-12-31, in the column order the model uses."""
    dim = build_dim_date(spark)
    assert dim.count() == 1096
    assert dim.select("date_key").distinct().count() == 1096
    assert dim.columns == OUTPUT_COLUMNS


def test_build_dim_date_accepts_dataframe_source(spark):
    """A DataFrame can be passed instead of a SparkSession."""
    df = spark.createDataFrame([("x",)], ["a"])
    dim = build_dim_date(df, "2018-01-01", "2018-01-03")
    assert dim.count() == 3
    assert row_for(dim, "2018-01-01")["date_key"] == 20180101


def test_black_friday_labels(spark):
    """Black Friday is the day after the 4th Thursday of November."""
    dim = build_dim_date(spark)
    bf_dates = [
        str(r["date"]) for r in dim.filter("is_black_friday = 'Black Friday'").collect()
    ]
    assert sorted(bf_dates) == ["2016-11-25", "2017-11-24", "2018-11-23"]
    assert row_for(dim, "2017-11-23")["is_black_friday"] == "Other Day"


def test_event_window_is_plus_minus_7_days(spark):
    dim = build_dim_date(spark, "2017-11-01", "2017-12-31")
    within = "Within 7 days of Black Friday"
    assert row_for(dim, "2017-11-17")["event_window"] == within
    assert row_for(dim, "2017-12-01")["event_window"] == within
    assert row_for(dim, "2017-11-16")["event_window"] == "More than 7 days away"
    assert row_for(dim, "2017-12-02")["event_window"] == "More than 7 days away"


def test_weekend_labels(spark):
    dim = build_dim_date(spark, "2017-11-24", "2017-11-27")
    assert row_for(dim, "2017-11-24")["is_weekend"] == "Weekday"
    assert row_for(dim, "2017-11-25")["is_weekend"] == "Weekend"
    assert row_for(dim, "2017-11-26")["is_weekend"] == "Weekend"


def test_event_periods_and_groups(spark):
    """Pre 28 / BF 3 / Post 28 days; Baseline = Pre + Post."""
    dim = build_dim_date(spark)
    counts = {
        (r["event_period_sort"], r["event_period"], r["period_group"]): r["count"]
        for r in dim.groupBy("event_period_sort", "event_period", "period_group")
        .count()
        .collect()
    }
    assert counts == {
        (1, "Pre 28 days", "Baseline"): 28,
        (2, "Black Friday 3 days", "Black Friday"): 3,
        (3, "Post 28 days", "Baseline"): 28,
        (4, "Other days", "Other days"): 1037,
    }
    assert row_for(dim, "2017-10-26")["event_period"] == "Other days"
    assert row_for(dim, "2017-10-27")["event_period"] == "Pre 28 days"
    assert row_for(dim, "2017-12-24")["event_period"] == "Post 28 days"
    assert row_for(dim, "2017-12-25")["event_period"] == "Other days"


def test_period_group_sort(spark):
    dim = build_dim_date(spark, "2017-11-20", "2017-11-30")
    assert row_for(dim, "2017-11-24")["period_group_sort"] == 1
    assert row_for(dim, "2017-11-20")["period_group_sort"] == 2
    assert row_for(dim, "2017-11-30")["period_group_sort"] == 2
