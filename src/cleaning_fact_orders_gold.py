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


def run_clean(spark) -> DataFrame:
    """Build and write the Gold order fact from the Silver orders table."""
    orders_df = spark.read.table("orders_silver")
    gold_df = clean_fact_orders_gold(orders_df)
    gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_fact_orders")
    print("Gold fact table 'gold_fact_orders' created successfully.")
    return gold_df


clean_orders_gold = clean_fact_orders_gold
