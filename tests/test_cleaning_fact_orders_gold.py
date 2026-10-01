from src.cleaning_fact_orders_gold import clean_fact_orders_gold


TEST_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def test_clean_fact_orders_gold_standardizes_and_deduplicates(spark):
    df = spark.createDataFrame(
        [
            (
                "ord_001",
                "cust_001",
                " Delivered ",
                "2024-01-01 10:00:00",
                "2024-01-01 09:00:00",
                "2024-01-02 12:00:00",
                "2024-01-03 15:00:00",
                "2024-01-05 08:00:00",
            ),
            (
                "ord_001",
                "cust_001",
                "delivered",
                "2024-01-01 10:00:00",
                "2024-01-01 09:00:00",
                "2024-01-02 12:00:00",
                "2024-01-03 15:00:00",
                "2024-01-05 08:00:00",
            ),
            (
                "ord_002",
                None,
                "created",
                "2024-01-02 10:00:00",
                None,
                None,
                None,
                "2024-01-06 08:00:00",
            ),
        ],
        TEST_COLUMNS,
    )

    result = clean_fact_orders_gold(df)

    assert result.count() == 1
    assert result.first()["order_status"] == "delivered"
    assert result.first()["order_id"] == "ord_001"
    assert result.columns == TEST_COLUMNS


def test_clean_fact_orders_gold_casts_timestamps_and_keeps_valid_rows(spark):
    df = spark.createDataFrame(
        [
            (
                "ord_003",
                "cust_003",
                "approved",
                "2024-02-10 12:00:00",
                "2024-02-10 11:00:00",
                None,
                None,
                "2024-02-15 12:00:00",
            ),
            (
                "ord_004",
                "cust_004",
                "shipped",
                "2999-01-01 00:00:00",
                None,
                None,
                None,
                "2024-02-16 12:00:00",
            ),
        ],
        TEST_COLUMNS,
    )

    result = clean_fact_orders_gold(df)

    assert result.count() == 1
    assert result.first()["order_id"] == "ord_003"
    assert str(result.first()["order_purchase_timestamp"]).endswith(
        "2024-02-10 12:00:00"
    )
