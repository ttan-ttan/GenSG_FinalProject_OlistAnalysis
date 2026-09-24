from src.cleaning_order_items import clean_order_items


def test_clean_order_items_basic(spark):
    """Check Order Items cleaning and type conversion."""

    df = spark.createDataFrame(
        [
            (
                " order001 ",
                "1",
                " product001 ",
                " seller001 ",
                "2017-10-10 15:00:00",
                "99.90",
                "12.50"
            )
        ],
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value"
        ]
    )

    cleaned = clean_order_items(df)
    row = cleaned.first()

    assert row["order_id"] == "order001"
    assert row["product_id"] == "product001"
    assert row["seller_id"] == "seller001"

    assert cleaned.schema["order_item_id"].dataType.typeName() == "integer"
    assert cleaned.schema["shipping_limit_date"].dataType.typeName() == "timestamp"
    assert cleaned.schema["price"].dataType.typeName() == "double"
    assert cleaned.schema["freight_value"].dataType.typeName() == "double"
