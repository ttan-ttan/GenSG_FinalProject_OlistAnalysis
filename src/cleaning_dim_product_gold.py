"""
Gold-layer build for the product dimension (dbo.gold_dim_product).

Grain: 1 row = 1 product (natural key product_id, no surrogate key).

Inputs (all Silver):
    - silver_products          products + native category
    - silver_product_category  native -> English category translation
    - silver_order_items       item prices (for the baseline)
    - silver_orders            order status + purchase date (for the baseline)

Team decisions:
    - All products kept, including those never sold (baseline NULL).
    - Left join to translation: missing / untranslated category -> English NULL.
    - English category formatted lowercase with spaces ("bed bath table");
      Silver translation delivers "Bed_bath_table" (initcap), so it is
      reformatted here.
    - English category only in output; native category dropped.
    - baseline_price_med = exact median item price per product, excluding
      canceled/unavailable orders and the 24-26 Nov 2017 event window.
"""

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

# Event window excluded from baseline (robustness window, inclusive)
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
    """Lowercase, trim, "_" / "-" / whitespace runs -> one space. Blank -> NULL."""
    text = F.trim(F.regexp_replace(F.lower(F.col(col_name)), r"[_\-\s]+", " "))
    return F.when(text == "", F.lit(None)).otherwise(text)


def add_category_english(products: DataFrame, translation: DataFrame) -> DataFrame:
    """Left join on the native category key. No match -> English NULL."""
    translation = translation.select(
        "product_category_name",
        format_english("product_category_name_english").alias(
            "product_category_name_english"
        ),
    ).dropDuplicates(["product_category_name"])
    return products.join(F.broadcast(translation), "product_category_name", "left")


def compute_baseline_price(items: DataFrame, orders: DataFrame) -> DataFrame:
    """Exact median item price per product; valid orders only; event excluded."""
    valid_orders = orders.filter(~F.col("order_status").isin(EXCLUDED_STATUSES)).select(
        "order_id", F.to_date("order_purchase_timestamp").alias("order_date")
    )
    non_event_items = items.join(valid_orders, "order_id", "inner").filter(
        ~F.col("order_date").between(EVENT_START, EVENT_END)
    )
    return non_event_items.groupBy("product_id").agg(
        F.round(F.expr("percentile(CAST(price AS DOUBLE), 0.5)"), 2).alias(
            "baseline_price_med"
        )
    )


def build_dim_product(
    products: DataFrame, translation: DataFrame, items: DataFrame, orders: DataFrame
) -> DataFrame:
    """Silver inputs -> gold_dim_product."""
    baseline = compute_baseline_price(items, orders)
    dim = add_category_english(products, translation).join(
        baseline, "product_id", "left"
    )
    return dim.select(*OUTPUT_COLUMNS)
