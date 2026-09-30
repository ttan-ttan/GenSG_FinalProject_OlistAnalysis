"""
Test Suite: Cleaning Logic for Products Dataset

Covers:
    1. Column rename (source typos) and type casting
    2. Text normalisation (trim, lowercase, empty -> null)
    3. Missing category -> NULL in both languages
    4. Translation cleaning and join (known untranslated -> English NULL)
    5. Missing dimensions / zero weight kept as-is
    6. Duplicate handling
    7. Required-column guard and output schema
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.cleaning_products import (
    OUTPUT_COLUMNS,
    SOURCE_COLUMNS,
    clean_products,
    clean_translation,
)

PID_1 = "1e9e8ef04dbcff4541ed26657ea517e5"
PID_2 = "3aa071139cb16b67ca9e5dea641aaa2f"
PID_3 = "96bd76ec8810374ed1b65e291975717f"

KNOWN_UNTRANSLATED = ["pc_gamer", "portateis_cozinha_e_preparadores_de_alimentos"]

PRODUCTS_SCHEMA = StructType(
    [StructField(c, StringType(), True) for c in SOURCE_COLUMNS]
)
TRANSLATION_SCHEMA = StructType(
    [
        StructField("product_category_name", StringType(), True),
        StructField("product_category_name_english", StringType(), True),
    ]
)


def _product(
    pid, category="perfumaria", weight="225", length="16", height="10", width="14"
):
    return (pid, category, "40", "287", "1", weight, length, height, width)


@pytest.fixture
def translation(spark):
    return spark.createDataFrame(
        [("perfumaria", "perfumery"), ("artes", "art"), (" Bebes ", " Baby ")],
        TRANSLATION_SCHEMA,
    )


def _clean(spark, rows, translation):
    products = spark.createDataFrame(rows, PRODUCTS_SCHEMA)
    return clean_products(products, translation)


def _by_id(df):
    return {r["product_id"]: r for r in df.collect()}


# 1. Rename + types
def test_output_columns_and_types(spark, translation):
    """Typo columns renamed, numeric columns cast, stable column order."""
    cleaned = _clean(spark, [_product(PID_1)], translation)

    assert cleaned.columns == OUTPUT_COLUMNS
    types = dict(cleaned.dtypes)
    for c in OUTPUT_COLUMNS[3:]:
        assert types[c] == "int", c
    assert types["product_id"] == "string"
    assert types["product_category_name_english"] == "string"
    assert "product_name_lenght" not in cleaned.columns
    assert "product_description_lenght" not in cleaned.columns


def test_values_cast_correctly(spark, translation):
    row = _clean(spark, [_product(PID_1)], translation).first()

    assert row["product_name_length"] == 40
    assert row["product_description_length"] == 287
    assert row["product_photos_qty"] == 1
    assert row["product_weight_g"] == 225
    assert (row["product_length_cm"], row["product_height_cm"]) == (16, 10)
    assert row["product_width_cm"] == 14


def test_unparseable_number_becomes_null(spark, translation):
    row = _clean(spark, [_product(PID_1, weight="abc")], translation).first()
    assert row["product_weight_g"] is None


# 2. Text normalisation
def test_text_normalisation(spark, translation):
    """product_id trimmed; category trimmed + lowercased, then translated."""
    cleaned = _clean(
        spark, [_product(f"  {PID_1} ", category="  PERFUMARIA ")], translation
    )
    row = cleaned.first()

    assert row["product_id"] == PID_1
    assert row["product_category_name"] == "perfumaria"
    assert row["product_category_name_english"] == "perfumery"


def test_blank_product_id_becomes_null(spark, translation):
    """Blank id -> NULL so validation rejects it (not silently kept as '')."""
    row = _clean(spark, [_product("   ")], translation).first()
    assert row["product_id"] is None


# 3. Missing category
@pytest.mark.parametrize("raw_category", [None, "", "   "])
def test_missing_category_null_in_both_languages(spark, translation, raw_category):
    """Null/blank category -> NULL native and NULL English (no 'unknown' fill)."""
    row = _clean(spark, [_product(PID_1, category=raw_category)], translation).first()

    assert row["product_category_name"] is None
    assert row["product_category_name_english"] is None


# 4. Translation
@pytest.mark.parametrize("category", KNOWN_UNTRANSLATED)
def test_known_untranslated_english_null(spark, translation, category):
    """No translation row -> English NULL, native category preserved."""
    row = _clean(spark, [_product(PID_1, category=category)], translation).first()

    assert row["product_category_name"] == category
    assert row["product_category_name_english"] is None


def test_translation_values_normalised(spark, translation):
    """Translation table padding/case does not break the join."""
    row = _clean(spark, [_product(PID_1, category="bebes")], translation).first()
    assert row["product_category_name_english"] == "baby"


def test_join_does_not_fan_out(spark):
    """Duplicate translation rows must not multiply products."""
    dup_translation = spark.createDataFrame(
        [("perfumaria", "perfumery"), ("PERFUMARIA ", "perfumery")], TRANSLATION_SCHEMA
    )
    rows = [_product(PID_1), _product(PID_2), _product(PID_3, category="artes")]
    assert _clean(spark, rows, dup_translation).count() == 3


def test_clean_translation(spark, translation):
    """BOM header handled, null keys dropped, values normalised."""
    bom = spark.createDataFrame(
        [("perfumaria", "perfumery"), (None, "orphan"), ("artes", None)],
        ["﻿product_category_name", "product_category_name_english"],
    )
    result = {r[0]: r[1] for r in clean_translation(bom).collect()}
    assert result == {"perfumaria": "perfumery"}

    normalised = {r[0]: r[1] for r in clean_translation(translation).collect()}
    assert normalised["bebes"] == "baby"


def test_translation_missing_column_raises(spark):
    bad = spark.createDataFrame([("perfumaria",)], ["product_category_name"])
    with pytest.raises(ValueError, match="translation: missing required column"):
        clean_translation(bad)


# 5. Missing dims / zero weight
def test_missing_dims_kept(spark, translation):
    """Row with a null dimension is kept, value stays NULL (no fill)."""
    rows = [_product(PID_1), _product(PID_2, height=None, weight=None)]
    result = _by_id(_clean(spark, rows, translation))

    assert len(result) == 2
    assert result[PID_2]["product_height_cm"] is None
    assert result[PID_2]["product_weight_g"] is None


def test_zero_weight_kept(spark, translation):
    row = _clean(spark, [_product(PID_1, weight="0")], translation).first()
    assert row["product_weight_g"] == 0


# 6. Duplicates
def test_exact_duplicates_dropped(spark, translation):
    cleaned = _clean(spark, [_product(PID_1), _product(PID_1)], translation)
    assert cleaned.count() == 1


def test_duplicates_after_normalisation_dropped(spark, translation):
    """Rows identical once trimmed/lowercased count as exact duplicates."""
    rows = [_product(PID_1), _product(f" {PID_1} ", category="PERFUMARIA")]
    assert _clean(spark, rows, translation).count() == 1


def test_conflicting_duplicates_kept_for_validation(spark, translation):
    """Same product_id, different values -> not silently resolved."""
    cleaned = _clean(
        spark, [_product(PID_1), _product(PID_1, weight="999")], translation
    )
    assert cleaned.count() == 2


# 7. Schema guards
def test_missing_required_column_raises(spark, translation):
    products = spark.createDataFrame(
        [(PID_1, "perfumaria")], ["product_id", "product_category_name"]
    )
    with pytest.raises(ValueError, match="products: missing required column"):
        clean_products(products, translation)


def test_bronze_metadata_columns_dropped(spark, translation):
    """Extra bronze columns do not leak into Silver or block exact-dup removal."""
    products = spark.createDataFrame(
        [_product(PID_1), _product(PID_1)], PRODUCTS_SCHEMA
    ).withColumn("_ingested_at", F.monotonically_increasing_id())
    cleaned = clean_products(products, translation)

    assert cleaned.columns == OUTPUT_COLUMNS
    assert cleaned.count() == 1


def test_bom_in_products_header_handled(spark, translation):
    products = spark.createDataFrame([_product(PID_1)], PRODUCTS_SCHEMA)
    products = products.withColumnRenamed("product_id", "﻿product_id ")
    assert clean_products(products, translation).first()["product_id"] == PID_1
