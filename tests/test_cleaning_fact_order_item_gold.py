""" test_cleaning_fact_order_item_gold  """

from src.cleaning_fact_order_item_gold import clean_fact_order_item_gold


def test_clean_fact_order_item_gold_filters_and_casts(spark):
    """Test that clean_fact_order_item_gold filters out invalid rows
    and casts columns to the correct types."""
    df = spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", "2018-01-01 00:00:00", 10.5, 2.0),
            ("o1", 1, "p1", "s1", "2018-01-01 00:00:00", 10.5, 2.0),
            ("o2", 1, "p2", None, "2018-01-02 00:00:00", 12.0, -1.0),
            ("o3", 2, "p3", "s3", "2018-01-03 00:00:00", None, 0.5),
        ],
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    )
    out = clean_fact_order_item_gold(df)
    assert out.count() == 1
    assert out.first()["order_id"] == "o1"
    assert out.first()["price"] == 10.5
    assert dict(out.dtypes)["order_item_id"] == "int"
    assert dict(out.dtypes)["shipping_limit_date"] == "timestamp"
    assert dict(out.dtypes)["price"] == "double"
    assert dict(out.dtypes)["freight_value"] == "double"


def test_clean_fact_order_item_gold_drops_invalid_item_ids_dates_and_amounts(spark):
    """Drop rows with invalid item IDs, shipping dates, or amount values."""
    df = spark.createDataFrame(
        [
            ("o1", 0, "p1", "s1", "2018-01-01 00:00:00", 10.0, 1.0),
            ("o2", None, "p2", "s2", "2018-01-01 00:00:00", 10.0, 1.0),
            ("o3", 1, "p3", "s3", "not-a-date", 10.0, 1.0),
            ("o4", 1, "p4", "s4", "2018-01-01 00:00:00", float("inf"), 1.0),
            ("o5", 1, "p5", "s5", "2018-01-01 00:00:00", 10.0, float("nan")),
        ],
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    )

    assert clean_fact_order_item_gold(df).count() == 0
