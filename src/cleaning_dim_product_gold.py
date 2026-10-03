"""
Gold-layer build for the product dimension (dbo.gold_dim_product).
"""

from operator import invert

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

EVENT_START = "2017-11-24"
EVENT_END = "2017-11-26"
EXCLUDED_STATUSES = ["canceled", "unavailable"]

OUTPUT_COLUMNS = [
    "product_id",
    "product_category_name_english",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "baseline_price_med",
]


def format_english(col_name: str) -> Column:
    """Normalize an English category to lowercase words separated by spaces."""
    text = F.trim(F.regexp_replace(F.lower(F.col(col_name)), r"[_\-\s]+", " "))
    return F.when(text == "", F.lit(None)).otherwise(text)


def add_category_english(products: DataFrame, translation: DataFrame) -> DataFrame:
    """Join each product to its normalized English category translation."""
    translation = translation.select(
        "product_category_name",
        format_english("product_category_name_english").alias(
            "product_category_name_english"
        ),
    ).dropDuplicates(["product_category_name"])

    return products.join(F.broadcast(translation), "product_category_name", "left")


def compute_baseline_price(items: DataFrame, orders: DataFrame) -> DataFrame:
    """Calculate each product's median non-event price from eligible orders."""
    valid_orders = orders.filter(
        invert(F.col("order_status").isin(EXCLUDED_STATUSES))
    ).select("order_id", F.to_date("order_purchase_timestamp").alias("order_date"))

    non_event_items = items.join(valid_orders, "order_id", "inner").filter(
        invert(F.col("order_date").between(EVENT_START, EVENT_END))
    )

    return non_event_items.groupBy("product_id").agg(
        F.round(F.expr("percentile(CAST(price AS DOUBLE), 0.5)"), 2).alias(
            "baseline_price_med"
        )
    )


def build_dim_product(products, translation, items, orders):
    """Build the product dimension with category translations and baseline prices."""
    baseline = compute_baseline_price(items, orders)
    dim = add_category_english(products, translation).join(
        baseline, "product_id", "left"
    )
    return dim.select(*OUTPUT_COLUMNS)


def run_clean(spark):
    """Entry point for Gold Runner Notebook."""
    products = spark.read.table("products_silver")
    translation = spark.read.table("product_category_name_silver")
    items = spark.read.table("order_items_silver")
    orders = spark.read.table("orders_silver")

    gold_df = build_dim_product(products, translation, items, orders)

    gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_dim_product")
    print("Gold dimension table 'gold_dim_product' created successfully.")
