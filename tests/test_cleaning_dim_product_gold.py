"""
Test Suite: Gold DimProduct build

Covers:
    1. Translation join (match, missing, untranslated, no fan-out)
    2. Baseline price (median, excluded statuses, event window, rounding)
    3. build_dim_product (all products kept, output schema)
"""

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.cleaning_dim_product_gold import (
    OUTPUT_COLUMNS,
    add_category_english,
    build_dim_product,
    compute_baseline_price,
)

P1, P2, P3 = "p1", "p2", "p3"

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType()),
        StructField("product_category_name", StringType()),
        StructField("product_name_length", IntegerType()),
        StructField("product_description_length", IntegerType()),
        StructField("product_photos_qty", IntegerType()),
        StructField("product_weight_g", IntegerType()),
        StructField("product_length_cm", IntegerType()),
        StructField("product_height_cm", IntegerType()),
        StructField("product_width_cm", IntegerType()),
    ]
)
TRANSLATION_SCHEMA = (
    "product_category_name string, product_category_name_english string"
)
ITEMS_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("order_item_id", IntegerType()),
        StructField("product_id", StringType()),
        StructField("price", DoubleType()),
    ]
)
ORDERS_SCHEMA = "order_id string, order_status string, order_purchase_timestamp string"


def _products(spark, rows):
    """rows: (product_id, category)"""
    full = [(pid, cat, 40, 287, 1, 225, 16, 10, 14) for pid, cat in rows]
    return spark.createDataFrame(full, PRODUCTS_SCHEMA)


@pytest.fixture(name="translation_data")
def _translation_fixture(spark):
    """Shaped like silver_product_category (clean_category_translation output)."""
    return spark.createDataFrame(
        [("perfumaria", "Perfumery"), ("cama mesa banho", "Bed_bath_table")],
        TRANSLATION_SCHEMA,
    )


def _english(df):
    """Map product IDs to their English categories."""
    return {r["product_id"]: r["product_category_name_english"] for r in df.collect()}


def _baseline(df):
    """Map product IDs to their calculated baseline prices."""
    return {r["product_id"]: r["baseline_price_med"] for r in df.collect()}


# 1. Translation join
def test_translation_match_and_misses(spark, translation_data):
    """Join translated, untranslated, and uncategorized products."""
    products = _products(spark, [(P1, "cama mesa banho"), (P2, "pc gamer"), (P3, None)])
    result = _english(add_category_english(products, translation_data))
    assert result == {P1: "bed bath table", P2: None, P3: None}


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Bed_bath_table", "bed bath table"),
        (" Fashio_female_clothing ", "fashio female clothing"),
        ("Home_appliances_2", "home appliances 2"),
        ("Perfumery", "perfumery"),
        ("  ", None),
    ],
)
def test_english_formatted_lowercase_with_spaces(spark, raw, expected):
    """Normalize English category values into lowercase space-separated text."""
    products = _products(spark, [(P1, "x")])
    translation_data = spark.createDataFrame([("x", raw)], TRANSLATION_SCHEMA)
    assert _english(add_category_english(products, translation_data)) == {P1: expected}


def test_translation_join_does_not_fan_out(spark):
    """Avoid multiplying products when translations contain duplicate keys."""
    dup = spark.createDataFrame(
        [("perfumaria", "perfumery"), ("perfumaria", "perfume")], TRANSLATION_SCHEMA
    )
    products = _products(spark, [(P1, "perfumaria"), (P2, "perfumaria")])
    assert add_category_english(products, dup).count() == 2


# 2. Baseline price
def _items_orders(spark, rows):
    """rows: (order_id, product_id, price, status, purchase_ts)"""
    items = spark.createDataFrame(
        [(o, 1, p, pr) for o, p, pr, _, _ in rows], ITEMS_SCHEMA
    )
    orders = spark.createDataFrame(
        list({(o, s, ts) for o, _, _, s, ts in rows}), ORDERS_SCHEMA
    )
    return items, orders


def test_baseline_is_median(spark):
    """Calculate the per-product median from eligible non-event orders."""
    items, orders = _items_orders(
        spark,
        [
            ("o1", P1, 10.0, "delivered", "2017-10-01 10:00:00"),
            ("o2", P1, 20.0, "delivered", "2017-10-02 10:00:00"),
            ("o3", P1, 100.0, "shipped", "2017-10-03 10:00:00"),
            ("o4", P2, 10.0, "delivered", "2017-10-01 10:00:00"),
            ("o5", P2, 15.0, "delivered", "2017-10-01 11:00:00"),
        ],
    )
    assert _baseline(compute_baseline_price(items, orders)) == {P1: 20.0, P2: 12.5}


@pytest.mark.parametrize("status", ["canceled", "unavailable"])
def test_baseline_excludes_statuses(spark, status):
    """Exclude canceled and unavailable orders from baseline calculations."""
    items, orders = _items_orders(
        spark,
        [
            ("o1", P1, 10.0, "delivered", "2017-10-01 10:00:00"),
            ("o2", P1, 999.0, status, "2017-10-02 10:00:00"),
        ],
    )
    assert _baseline(compute_baseline_price(items, orders)) == {P1: 10.0}


@pytest.mark.parametrize(
    "ts, excluded",
    [
        ("2017-11-23 23:59:59", False),
        ("2017-11-24 00:00:00", True),
        ("2017-11-26 23:59:59", True),
        ("2017-11-27 00:00:00", False),
    ],
)
def test_baseline_event_window_boundaries(spark, ts, excluded):
    """Exclude purchases inside the event window, including its boundaries."""
    items, orders = _items_orders(
        spark,
        [
            ("o1", P1, 10.0, "delivered", "2017-10-01 10:00:00"),
            ("o2", P1, 30.0, "delivered", ts),
        ],
    )
    expected = 10.0 if excluded else 20.0
    assert _baseline(compute_baseline_price(items, orders)) == {P1: expected}


def test_baseline_rounded_to_2dp(spark):
    """Round the calculated product median to two decimal places."""
    items, orders = _items_orders(
        spark,
        [
            ("o1", P1, 10.001, "delivered", "2017-10-01 10:00:00"),
            ("o2", P1, 10.004, "delivered", "2017-10-02 10:00:00"),
        ],
    )
    assert _baseline(compute_baseline_price(items, orders)) == {P1: 10.0}


# 3. build_dim_product
def test_build_keeps_all_products_and_schema(spark, translation_data):
    """Keep every product and return the documented output columns."""
    products = _products(spark, [(P1, "perfumaria"), (P2, None)])
    items, orders = _items_orders(
        spark,
        [
            ("o1", P1, 10.0, "delivered", "2017-10-01 10:00:00"),
            ("o2", P1, 20.0, "delivered", "2017-11-24 10:00:00"),
        ],
    )
    dim = build_dim_product(products, translation_data, items, orders)

    assert dim.columns == OUTPUT_COLUMNS
    rows = {r["product_id"]: r for r in dim.collect()}
    assert set(rows) == {P1, P2}
    assert rows[P1]["product_category_name_english"] == "perfumery"
    assert rows[P1]["baseline_price_med"] == 10.0
    assert rows[P2]["product_category_name_english"] is None
    assert rows[P2]["baseline_price_med"] is None  # never sold -> kept, NULL


def test_build_product_only_sold_in_event_has_null_baseline(spark, translation_data):
    """Leave the baseline null when a product sold only during the event."""
    products = _products(spark, [(P1, "perfumaria")])
    items, orders = _items_orders(
        spark, [("o1", P1, 10.0, "delivered", "2017-11-25 10:00:00")]
    )
    row = build_dim_product(products, translation_data, items, orders).first()
    assert row["baseline_price_med"] is None
