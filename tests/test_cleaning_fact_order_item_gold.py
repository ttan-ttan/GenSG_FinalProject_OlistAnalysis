from src.cleaning_fact_order_item_gold import clean_fact_order_item_gold


def test_clean_fact_order_item_gold_filters_and_casts(spark):
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
