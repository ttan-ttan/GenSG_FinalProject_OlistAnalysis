"""
cleaning_dim_product_gold
Gold-layer build for the product dimension.
"""

from operator import invert
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window


EVENT_START = "2017-11-24"
EVENT_END = "2017-11-26"
EXCLUDED_STATUSES = ["canceled", "unavailable"]

OUTPUT_COLUMNS = [
    "product_key", "product_id", "category_pt", "category_en",
    "weight_g", "length_cm", "height_cm", "width_cm", "baseline_price_med"
]


def format_english(col_name: str):
    text = F.trim(F.regexp_replace(F.lower(F.col(col_name)), r"[_\-\s]+", " "))
    return F.when(text == "", F.lit(None)).otherwise(text)


def add_category_english(products: DataFrame, translation: DataFrame) -> DataFrame:
    translation = (
        translation.select(
            "product_category_name",
            format_english("product_category_name_english").alias(
                "product_category_name_english"),
        )
        .dropDuplicates(["product_category_name"])
    )
    return products.join(F.broadcast(translation), "product_category_name", "left")


def compute_baseline_price(items: DataFrame, orders: DataFrame) -> DataFrame:
    valid_orders = (
        orders.filter(invert(F.col("order_status").isin(EXCLUDED_STATUSES)))
              .select("order_id", F.to_date("order_purchase_timestamp").alias("order_date"))
    )

    non_event_items = (
        items.join(valid_orders, "order_id", "inner")
             .filter(invert(F.col("order_date").between(EVENT_START, EVENT_END)))
    )

    return non_event_items.groupBy("product_id").agg(
        F.round(F.expr("percentile(CAST(price AS DOUBLE), 0.5)"),
                2).alias("baseline_price_med")
    )


def build_dim_product_gold(products, translation, items, orders):
    baseline = compute_baseline_price(items, orders)

    dim = (
        add_category_english(products, translation)
        .join(baseline, "product_id", "left")
        .withColumnRenamed("product_category_name", "category_pt")
        .withColumnRenamed("product_category_name_english", "category_en")
        .withColumnRenamed("product_weight_g", "weight_g")
        .withColumnRenamed("product_length_cm", "length_cm")
        .withColumnRenamed("product_height_cm", "height_cm")
        .withColumnRenamed("product_width_cm", "width_cm")
        .withColumn("product_key", F.row_number().over(Window.orderBy("product_id")))
    )

    return dim.select(*OUTPUT_COLUMNS)


def run_clean(spark):
    products = spark.read.table("silver_products")
    translation = spark.read.table("silver_product_category_name")
    items = spark.read.table("silver_order_items")
    orders = spark.read.table("silver_orders")

    gold_df = build_dim_product_gold(products, translation, items, orders)

    gold_df.write.format("delta").mode(
        "overwrite").saveAsTable("gold_dim_product")
    print("Gold dimension table 'gold_dim_product' created successfully.")
    return gold_df


# Alias for tests
build_dim_product = build_dim_product_gold
