""" test_cleaning_fact_orders_gold  """

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from src.cleaning_fact_orders_gold import build_fact_orders_gold, clean_fact_orders_gold
from src.cleaning_dim_date_gold import build_dim_date

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

TEST_SCHEMA = StructType(
    [StructField(column, StringType(), True) for column in TEST_COLUMNS]
)


def test_clean_fact_orders_gold_standardizes_and_deduplicates(spark):
    """Test that clean_fact_orders_gold standardizes the order_status and removes duplicates."""
    df = spark.createDataFrame(
        [
            (
                "ord_001",
                "cust_001",
                " Delivered ",
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-02 12:00:00",
                "2024-01-03 15:00:00",
                "2024-01-05 08:00:00",
            ),
            (
                "ord_001",
                "cust_001",
                "delivered",
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
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
        TEST_SCHEMA,
    )

    result = clean_fact_orders_gold(df)

    assert result.count() == 1
    assert result.first()["order_status"] == "delivered"
    assert result.first()["order_id"] == "ord_001"
    assert result.columns == TEST_COLUMNS


def test_clean_fact_orders_gold_casts_timestamps_and_keeps_valid_rows(spark):
    """Test that clean_fact_orders_gold casts timestamp columns and keeps only valid rows."""
    df = spark.createDataFrame(
        [
            (
                "ord_003",
                "cust_003",
                "approved",
                "2024-02-10 12:00:00",
                "2024-02-10 13:00:00",
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
        TEST_SCHEMA,
    )

    result = clean_fact_orders_gold(df)

    assert result.count() == 1
    assert result.first()["order_id"] == "ord_003"
    assert str(result.first()["order_purchase_timestamp"]).endswith(
        "2024-02-10 12:00:00"
    )


def test_build_fact_orders_gold_aggregates_and_resolves_keys(spark):
    orders = spark.createDataFrame(
        [
            (
                "o1",
                "c1",
                "delivered",
                "2017-11-24 10:00:00",
                "2017-11-24 11:00:00",
                "2017-11-25 10:00:00",
                "2017-11-27 10:00:00",
                "2017-11-26 10:00:00",
            )
        ],
        TEST_COLUMNS,
    )
    customers = spark.createDataFrame(
        [("c1", "u1")], ["customer_id", "customer_unique_id"]
    )
    dim_customer = spark.createDataFrame(
        [(7, "u1", "2017-11-24", "SP", "sao paulo")],
        ["customer_key", "customer_unique_id", "first_order_date", "state", "city"],
    ).withColumn("first_order_date", F.to_date("first_order_date"))
    items = spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", "2017-11-24 12:00:00", 10.0, 1.0),
            ("o1", 2, "p2", "s2", "2017-11-24 12:00:00", 20.0, 2.0),
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
    payments = spark.createDataFrame([("o1", 30.0)], ["order_id", "payment_value"])
    dim_date = build_dim_date(orders)

    result = build_fact_orders_gold(
        orders, items, payments, customers, dim_customer, dim_date
    ).first()

    assert result["date_key"] == 20171124
    assert result["customer_key"] == 7
    assert result["item_count"] == 2
    assert result["order_value"] == 30.0
    assert result["payment_value_total"] == 30.0
    assert result["delivery_delay_days"] == 1
    assert result["is_new_at_order"] is True
