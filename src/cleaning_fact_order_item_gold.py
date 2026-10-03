""" cleaning_fact_order_item_gold
This module contains functions to clean the Gold fact order-item table."""

from __future__ import annotations

from operator import invert
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
]

FACT_COLUMNS = [
    "order_item_id",
    "order_id",
    "date_key",
    "customer_key",
    "product_key",
    "seller_key",
    "price",
    "freight_value",
    "price_vs_baseline_pct",
]


def clean_fact_order_item_gold(df: DataFrame) -> DataFrame:
    """Clean the Gold fact order-item table."""
    df = df.select(*[c.strip().lower() if c in df.columns else c for c in df.columns])
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = (
        df.withColumn("order_id", F.trim(F.col("order_id").cast("string")))
        .withColumn("product_id", F.trim(F.col("product_id").cast("string")))
        .withColumn("seller_id", F.trim(F.col("seller_id").cast("string")))
        .withColumn("order_item_id", F.col("order_item_id").cast("int"))
        .withColumn(
            "shipping_limit_date", F.try_to_timestamp(F.col("shipping_limit_date"))
        )
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
    )

    df = df.filter(F.col("order_id").isNotNull())
    df = df.filter(F.col("order_item_id").isNotNull())
    df = df.filter(F.col("order_item_id") > 0)
    df = df.filter(F.col("product_id").isNotNull())
    df = df.filter(F.col("seller_id").isNotNull())
    df = df.filter(F.col("shipping_limit_date").isNotNull())
    df = df.filter(F.col("price").isNotNull())
    df = df.filter(F.col("freight_value").isNotNull())
    df = df.filter(invert(F.isnan("price")))
    df = df.filter(invert(F.isnan("freight_value")))
    df = df.filter(F.abs(F.col("price")) != float("inf"))
    df = df.filter(F.abs(F.col("freight_value")) != float("inf"))
    df = df.filter(F.col("freight_value") >= 0)
    df = df.filter(F.col("price") > 0)
    df = df.dropDuplicates()

    return df.select(*REQUIRED_COLUMNS)


def build_fact_order_items_gold(
    items: DataFrame,
    fact_orders: DataFrame,
    dim_product: DataFrame,
    dim_seller: DataFrame,
) -> DataFrame:
    """Build one Gold fact row per order item and resolve all declared FKs."""
    clean_items = clean_fact_order_item_gold(items)
    orders = fact_orders.select("order_id", "date_key", "customer_key").dropDuplicates(
        ["order_id"]
    )
    products = dim_product.select("product_id", "product_key", "baseline_price_med")
    sellers = dim_seller.select("seller_id", "seller_key")

    joined = (
        clean_items.join(orders, "order_id", "left")
        .join(products, "product_id", "left")
        .join(sellers, "seller_id", "left")
    )
    if (
        joined.filter(
            F.col("date_key").isNull()
            | F.col("customer_key").isNull()
            | F.col("product_key").isNull()
            | F.col("seller_key").isNull()
        )
        .limit(1)
        .count()
    ):
        raise ValueError("Order-item fact contains an orphan order, product, or seller")

    baseline = F.col("baseline_price_med").cast("double")
    return joined.withColumn(
        "price_vs_baseline_pct",
        F.when(
            baseline.isNotNull() & (baseline > 0),
            ((baseline - F.col("price")) / baseline) * F.lit(100.0),
        ).cast("double"),
    ).select(*FACT_COLUMNS)


clean_order_item_gold = clean_fact_order_item_gold


def run_clean(spark) -> DataFrame:
    """Build and write the Gold order-item fact from Silver and Gold tables."""
    items_df = spark.read.table("order_items_silver")
    gold_df = build_fact_order_items_gold(
        items_df,
        spark.read.table("fact_order"),
        spark.read.table("dim_product"),
        spark.read.table("dim_seller"),
    )
    gold_df.write.format("delta").mode("overwrite").saveAsTable("fact_order_item")
    print("Gold fact table 'fact_order_item' created successfully.")
    return gold_df
