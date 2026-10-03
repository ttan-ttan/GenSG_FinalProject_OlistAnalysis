"""Gold-layer cleaning for the Olist order-level fact table."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

FACT_COLUMNS = [
    "order_id",
    "date_key",
    "customer_key",
    "order_value",
    "item_count",
    "delivery_delay_days",
    "payment_value_total",
    "is_new_at_order",
]


def clean_fact_orders_gold(df: DataFrame) -> DataFrame:
    """Clean order-level rows for the Gold fact table."""
    df = df.toDF(*[column.strip().lower() for column in df.columns])
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.select(*REQUIRED_COLUMNS)
    df = (
        df.withColumn("order_id", F.trim(F.col("order_id").cast("string")))
        .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
        .withColumn(
            "order_status",
            F.lower(F.trim(F.col("order_status").cast("string"))),
        )
        .withColumn(
            "order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp")
        )
        .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
        .withColumn(
            "order_delivered_carrier_date",
            F.to_timestamp("order_delivered_carrier_date"),
        )
        .withColumn(
            "order_delivered_customer_date",
            F.to_timestamp("order_delivered_customer_date"),
        )
        .withColumn(
            "order_estimated_delivery_date",
            F.to_timestamp("order_estimated_delivery_date"),
        )
    )

    df = (
        df.filter(F.col("order_id").isNotNull() & (F.trim("order_id") != ""))
        .filter(F.col("customer_id").isNotNull() & (F.trim("customer_id") != ""))
        .filter(F.col("order_status").isNotNull() & (F.col("order_status") != ""))
        .filter(F.col("order_purchase_timestamp").isNotNull())
        .filter(F.col("order_purchase_timestamp") <= F.current_timestamp())
        .filter(
            F.col("order_approved_at").isNull()
            | (F.col("order_approved_at") >= F.col("order_purchase_timestamp"))
        )
        .filter(
            F.col("order_delivered_carrier_date").isNull()
            | F.col("order_approved_at").isNull()
            | (F.col("order_delivered_carrier_date") >= F.col("order_approved_at"))
        )
        .filter(
            F.col("order_delivered_customer_date").isNull()
            | F.col("order_delivered_carrier_date").isNull()
            | (
                F.col("order_delivered_customer_date")
                >= F.col("order_delivered_carrier_date")
            )
        )
        .filter(
            F.col("order_estimated_delivery_date").isNull()
            | (
                F.col("order_estimated_delivery_date")
                >= F.col("order_purchase_timestamp")
            )
        )
    )

    return df.dropDuplicates(["order_id"]).select(*REQUIRED_COLUMNS)


def build_fact_orders_gold(
    orders: DataFrame,
    order_items: DataFrame,
    payments: DataFrame,
    customers: DataFrame,
    dim_customer: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    """Build one Gold fact row per order, resolving dimension keys and measures."""
    clean_orders = clean_fact_orders_gold(orders)
    customer_lookup = (
        customers.select("customer_id", "customer_unique_id")
        .dropDuplicates(["customer_id"])
        .join(
            dim_customer.select(
                "customer_unique_id", "customer_key", "first_order_date"
            ),
            "customer_unique_id",
            "left",
        )
        .select("customer_id", "customer_key", "first_order_date")
    )
    date_lookup = dim_date.select(F.col("date").alias("_date"), "date_key")
    item_totals = order_items.groupBy("order_id").agg(
        F.countDistinct("order_item_id").alias("item_count"),
        F.sum(F.col("price").cast("double")).alias("order_value"),
    )
    payment_totals = payments.groupBy("order_id").agg(
        F.sum(F.col("payment_value").cast("double")).alias("payment_value_total")
    )

    fact = (
        clean_orders.join(customer_lookup, "customer_id", "left")
        .join(
            date_lookup,
            F.to_date("order_purchase_timestamp") == F.col("_date"),
            "left",
        )
        .join(item_totals, "order_id", "left")
        .join(payment_totals, "order_id", "left")
        .withColumn(
            "delivery_delay_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull()
                & F.col("order_estimated_delivery_date").isNotNull(),
                F.datediff(
                    F.to_date("order_delivered_customer_date"),
                    F.to_date("order_estimated_delivery_date"),
                ),
            ).cast("int"),
        )
        .withColumn(
            "is_new_at_order",
            F.to_date("order_purchase_timestamp") == F.col("first_order_date"),
        )
        .drop("_date", "first_order_date")
    )
    missing_customer = fact.filter(F.col("customer_key").isNull()).limit(1).count()
    missing_date = fact.filter(F.col("date_key").isNull()).limit(1).count()
    if missing_customer or missing_date:
        raise ValueError("Orders fact contains an unmapped customer or purchase date")

    return fact.select(
        *[
            (
                F.coalesce(F.col(column), F.lit(0)).alias(column)
                if column in {"item_count", "order_value", "payment_value_total"}
                else F.col(column)
            )
            for column in FACT_COLUMNS
        ]
    )


def run_clean(spark) -> DataFrame:
    """Build and write the Gold order fact from Silver inputs and dimensions."""
    orders_df = spark.read.table("orders_silver")
    gold_df = build_fact_orders_gold(
        orders_df,
        spark.read.table("order_items_silver"),
        spark.read.table("order_payments_silver"),
        spark.read.table("customers_silver"),
        spark.read.table("dim_customer"),
        spark.read.table("dim_date"),
    )
    gold_df.write.format("delta").mode("overwrite").saveAsTable("fact_order")
    print("Gold fact table 'fact_order' created successfully.")
    return gold_df


clean_orders_gold = clean_fact_orders_gold
