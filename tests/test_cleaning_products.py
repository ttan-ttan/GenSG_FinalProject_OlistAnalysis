"""
Test Suite: Cleaning Logic for Products Dataset

Covers:
    1. Column rename (source typos) and type casting
    2. Text normalisation (trim, lowercase, empty -> null)
    3. Missing-category / missing-dimension flags and fill
    4. Derived product_volume_cm3
    5. Translation cleaning, manual translations, join
    6. Duplicate handling
    7. Required-column guard and output schema
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.cleaning_products import (
    KNOWN_UNTRANSLATED,
    OUTPUT_COLUMNS,
    SOURCE_COLUMNS,
    UNKNOWN_CATEGORY,
    clean_products,
    clean_translation,
)

PID_1 = "1e9e8ef04dbcff4541ed26657ea517e5"
PID_2 = "3aa071139cb16b67ca9e5dea641aaa2f"
PID_3 = "96bd76ec8810374ed1b65e291975717f"

# Bronze lands as strings -> mimic that
PRODUCTS_SCHEMA = StructType([StructField(c, StringType(), True) for c in SOURCE_COLUMNS])
TRANSLATION_SCHEMA = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
])


def _product(pid, category="perfumaria", weight="225", length="16", height="10", width="14"):
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


def test_output_columns_and_types(spark, translation):
    """Typo columns renamed, numeric columns cast, stable column order."""
    cleaned = _clean(spark, [_product(PID_1)], translation)

    assert cleaned.columns == OUTPUT_COLUMNS
    types = dict(cleaned.dtypes)
    assert types["product_name_length"] == "int"
    assert types["product_weight_g"] == "int"
    assert types["product_volume_cm3"] == "bigint"
    assert types["is_category_missing"] == "boolean"
    assert "product_name_lenght" not in cleaned.columns


def test_text_normalisation(spark, translation):
    """product_id trimmed; category trimmed + lowercased."""
    cleaned = _clean(spark, [_product(f"  {PID_1} ", category="  PERFUMARIA ")], translation)
    row = cleaned.first()

    assert row["product_id"] == PID_1
    assert row["product_category_name"] == "perfumaria"
    assert row["product_category_name_english"] == "perfumery"


@pytest.mark.parametrize("raw_category", [None, "", "   "])
def test_missing_category_flagged_and_filled(spark, translation, raw_category):
    """Null/blank category -> 'unknown' in both languages, flag set."""
    row = _clean(spark, [_product(PID_1, category=raw_category)], translation).first()

    assert row["is_category_missing"] is True
    assert row["product_category_name"] == UNKNOWN_CATEGORY
    assert row["product_category_name_english"] == UNKNOWN_CATEGORY


def test_missing_dims_kept_and_flagged(spark, translation):
    """Row with a null dimension is kept, flagged, volume null."""
    rows = [_product(PID_1), _product(PID_2, height=None)]
    result = _by_id(_clean(spark, rows, translation))

    assert len(result) == 2
    assert result[PID_1]["is_dims_missing"] is False
    assert result[PID_2]["is_dims_missing"] is True
    assert result[PID_2]["product_volume_cm3"] is None


def test_missing_weight_only_flags_dims_but_keeps_volume(spark, translation):
    """Weight is part of the dims flag but not of volume."""
    row = _clean(spark, [_product(PID_1, weight=None)], translation).first()

    assert row["is_dims_missing"] is True
    assert row["product_volume_cm3"] == 16 * 10 * 14


def test_zero_weight_kept(spark, translation):
    """Weight 0 is kept as-is."""
    row = _clean(spark, [_product(PID_1, weight="0")], translation).first()
    assert row["product_weight_g"] == 0


def test_volume_no_int_overflow(spark, translation):
    """Large dimensions do not overflow int."""
    row = _clean(spark, [_product(PID_1, length="2000", height="2000", width="2000")],
                 translation).first()
    assert row["product_volume_cm3"] == 8_000_000_000


def test_unparseable_number_becomes_null(spark, translation):
    row = _clean(spark, [_product(PID_1, weight="abc")], translation).first()
    assert row["product_weight_g"] is None


def test_known_untranslated_becomes_unknown(spark, translation):
    """Categories with no official translation -> English 'unknown', flag stays False."""
    rows = [_product(pid, category=c)
            for pid, c in zip([PID_1, PID_2], sorted(KNOWN_UNTRANSLATED))]
    result = _by_id(_clean(spark, rows, translation))

    assert len(result) == 2
    for row in result.values():
        assert row["product_category_name"] in KNOWN_UNTRANSLATED
        assert row["product_category_name_english"] == UNKNOWN_CATEGORY
        assert row["is_category_missing"] is False


def test_unmapped_category_becomes_unknown_but_not_flagged(spark, translation):
    """New untranslated category -> english 'unknown', flag stays False (validation catches it)."""
    row = _clean(spark, [_product(PID_1, category="nova_categoria")], translation).first()

    assert row["product_category_name"] == "nova_categoria"
    assert row["product_category_name_english"] == UNKNOWN_CATEGORY
    assert row["is_category_missing"] is False


def test_exact_duplicates_dropped(spark, translation):
    cleaned = _clean(spark, [_product(PID_1), _product(PID_1)], translation)
    assert cleaned.count() == 1


def test_conflicting_duplicates_kept_for_validation(spark, translation):
    """Same product_id, different values -> not silently resolved."""
    cleaned = _clean(spark, [_product(PID_1), _product(PID_1, weight="999")], translation)
    assert cleaned.count() == 2


def test_join_does_not_fan_out(spark):
    """Duplicate translation rows must not multiply products."""
    dup_translation = spark.createDataFrame(
        [("perfumaria", "perfumery"), ("PERFUMARIA ", "perfumery")], TRANSLATION_SCHEMA)
    rows = [_product(PID_1), _product(PID_2), _product(PID_3, category="perfumaria")]
    assert _clean(spark, rows, dup_translation).count() == 3


def test_clean_translation(spark, translation):
    """Normalised, deduped, manual rows appended, BOM header handled."""
    bom = spark.createDataFrame(
        [("perfumaria", "perfumery"), (None, "orphan")],
        ["\ufeffproduct_category_name", "product_category_name_english"],
    )
    result = {r[0]: r[1] for r in clean_translation(bom).collect()}

    assert result["perfumaria"] == "perfumery"
    assert None not in result
    assert len(result) == 1

    normalised = {r[0]: r[1] for r in clean_translation(translation).collect()}
    assert normalised["bebes"] == "baby"


def test_missing_required_column_raises(spark, translation):
    products = spark.createDataFrame([(PID_1, "perfumaria")],
                                     ["product_id", "product_category_name"])
    with pytest.raises(ValueError, match="missing required column"):
        clean_products(products, translation)


def test_bronze_metadata_columns_dropped(spark, translation):
    """Extra bronze columns do not leak into Silver."""
    products = (spark.createDataFrame([_product(PID_1)], PRODUCTS_SCHEMA)
                .withColumn("_ingested_at", F.current_timestamp()))
    assert clean_products(products, translation).columns == OUTPUT_COLUMNS

