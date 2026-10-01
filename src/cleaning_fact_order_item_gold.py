from __future__ import annotations

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
        .withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
    )

    df = df.filter(F.col("order_id").isNotNull())
    df = df.filter(F.col("product_id").isNotNull())
    df = df.filter(F.col("seller_id").isNotNull())
    df = df.filter(F.col("price").isNotNull())
    df = df.filter(F.col("freight_value").isNotNull())
    df = df.filter(F.col("freight_value") >= 0)
    df = df.filter(F.col("price") > 0)
    df = df.dropDuplicates()

    return df.select(*REQUIRED_COLUMNS)


clean_order_item_gold = clean_fact_order_item_gold
