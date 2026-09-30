"""
Test Suite: Cleaning Logic for Products Dataset

Covers:
    1. Column rename (source typos) and type casting
    2. Text normalisation (trim, lowercase, "_" -> space, empty -> null)
    3. Missing category -> NULL
    4. No translation join in Silver (moved to Gold)
    5. Missing dimensions / zero weight kept as-is
    6. Duplicate handling
    7. Required-column guard and output schema
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.cleaning_products import OUTPUT_COLUMNS, SOURCE_COLUMNS, clean_products

PID_1 = "1e9e8ef04dbcff4541ed26657ea517e5"
PID_2 = "3aa071139cb16b67ca9e5dea641aaa2f"

PRODUCTS_SCHEMA = StructType(
    [StructField(c, StringType(), True) for c in SOURCE_COLUMNS]
)


def _product(
    pid, category="perfumaria", weight="225", length="16", height="10", width="14"
):
    return (pid, category, "40", "287", "1", weight, length, height, width)


def _clean(spark, rows):
    return clean_products(spark.createDataFrame(rows, PRODUCTS_SCHEMA))


def _by_id(df):
    return {r["product_id"]: r for r in df.collect()}


# 1. Rename + types
def test_output_columns_and_types(spark):
    """Typo columns renamed, numeric columns cast, stable column order."""
    cleaned = _clean(spark, [_product(PID_1)])

    assert cleaned.columns == OUTPUT_COLUMNS
    types = dict(cleaned.dtypes)
    for c in OUTPUT_COLUMNS[2:]:
        assert types[c] == "int", c
    assert types["product_id"] == "string"
    assert types["product_category_name"] == "string"
    assert "product_name_lenght" not in cleaned.columns
    assert "product_description_lenght" not in cleaned.columns


def test_values_cast_correctly(spark):
    row = _clean(spark, [_product(PID_1)]).first()

    assert row["product_name_length"] == 40
    assert row["product_description_length"] == 287
    assert row["product_photos_qty"] == 1
    assert row["product_weight_g"] == 225
    assert (row["product_length_cm"], row["product_height_cm"]) == (16, 10)
    assert row["product_width_cm"] == 14


def test_unparseable_number_becomes_null(spark):
    row = _clean(spark, [_product(PID_1, weight="abc")]).first()
    assert row["product_weight_g"] is None


# 2. Text normalisation
def test_product_id_trimmed(spark):
    row = _clean(spark, [_product(f"  {PID_1} ")]).first()
    assert row["product_id"] == PID_1


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("  PERFUMARIA ", "perfumaria"),
        ("cama_mesa_banho", "cama mesa banho"),
        (" Cama__Mesa-Banho ", "cama mesa banho"),
        ("eletrodomesticos_2", "eletrodomesticos 2"),
    ],
)
def test_category_normalised_to_translation_format(spark, raw, expected):
    """Same format as silver_product_category key so the Gold join matches."""
    row = _clean(spark, [_product(PID_1, category=raw)]).first()
    assert row["product_category_name"] == expected


def test_blank_product_id_becomes_null(spark):
    """Blank id -> NULL so validation rejects it (not silently kept as '')."""
    row = _clean(spark, [_product("   ")]).first()
    assert row["product_id"] is None


# 3. Missing category
@pytest.mark.parametrize("raw_category", [None, "", "   "])
def test_missing_category_null(spark, raw_category):
    """Null/blank category -> NULL (no 'unknown' fill)."""
    row = _clean(spark, [_product(PID_1, category=raw_category)]).first()
    assert row["product_category_name"] is None


# 4. No translation in Silver
def test_no_english_column(spark):
    cleaned = _clean(spark, [_product(PID_1)])
    assert "product_category_name_english" not in cleaned.columns


# 5. Missing dims / zero weight
def test_missing_dims_kept(spark):
    """Row with a null dimension is kept, value stays NULL (no fill)."""
    rows = [_product(PID_1), _product(PID_2, height=None, weight=None)]
    result = _by_id(_clean(spark, rows))

    assert len(result) == 2
    assert result[PID_2]["product_height_cm"] is None
    assert result[PID_2]["product_weight_g"] is None


def test_zero_weight_kept(spark):
    row = _clean(spark, [_product(PID_1, weight="0")]).first()
    assert row["product_weight_g"] == 0


# 6. Duplicates
def test_exact_duplicates_dropped(spark):
    assert _clean(spark, [_product(PID_1), _product(PID_1)]).count() == 1


def test_duplicates_after_normalisation_dropped(spark):
    """Rows identical once trimmed/lowercased count as exact duplicates."""
    rows = [_product(PID_1), _product(f" {PID_1} ", category="PERFUMARIA")]
    assert _clean(spark, rows).count() == 1


def test_conflicting_duplicates_kept_for_validation(spark):
    """Same product_id, different values -> not silently resolved."""
    rows = [_product(PID_1), _product(PID_1, weight="999")]
    assert _clean(spark, rows).count() == 2


# 7. Schema guards
def test_missing_required_column_raises(spark):
    products = spark.createDataFrame(
        [(PID_1, "perfumaria")], ["product_id", "product_category_name"]
    )
    with pytest.raises(ValueError, match="products: missing required column"):
        clean_products(products)


def test_bronze_metadata_columns_dropped(spark):
    """Extra bronze columns do not leak into Silver or block exact-dup removal."""
    products = spark.createDataFrame(
        [_product(PID_1), _product(PID_1)], PRODUCTS_SCHEMA
    ).withColumn("_ingested_at", F.monotonically_increasing_id())
    cleaned = clean_products(products)

    assert cleaned.columns == OUTPUT_COLUMNS
    assert cleaned.count() == 1


def test_bom_in_products_header_handled(spark):
    products = spark.createDataFrame([_product(PID_1)], PRODUCTS_SCHEMA)
    products = products.withColumnRenamed("product_id", "﻿product_id ")
    assert clean_products(products).first()["product_id"] == PID_1
