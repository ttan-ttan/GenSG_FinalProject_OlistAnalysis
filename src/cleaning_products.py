"""
Silver-layer cleaning for the products dataset.

Casts types, fixes source column typos, normalises text, flags incomplete
records, derives product volume and attaches the English category name
from product_category_name_translation.

Input : bronze products + bronze translation DataFrames (string or typed columns).
Output: one row per product_id. Run validation_products.validate_products before writing.

Rules:
    - Missing category -> "unknown", flagged with is_category_missing.
    - Categories with no official translation -> English "unknown".
    - Rows with missing dimensions are kept and flagged with is_dims_missing.
    - product_weight_g == 0 is kept as-is.
    - Only exact duplicate rows are dropped. Conflicting rows sharing a
      product_id are left for validation to reject.
"""

from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import IntegerType, LongType, StringType

# Source column typos -> fixed names
RENAMES = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
}

SOURCE_COLUMNS = [
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]

TRANSLATION_COLUMNS = ["product_category_name", "product_category_name_english"]

INT_COLS = [
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]

DIM_COLS = [
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]
SIZE_COLS = ["product_length_cm", "product_height_cm", "product_width_cm"]

UNKNOWN_CATEGORY = "unknown"

# Categories with no official Olist translation -> English "unknown".
# Listed so validation can tell a KNOWN gap from a NEW one.
KNOWN_UNTRANSLATED = {"pc_gamer", "portateis_cozinha_e_preparadores_de_alimentos"}

OUTPUT_COLUMNS = [
    "product_id",
    "product_category_name",
    "product_category_name_english",
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "product_volume_cm3",
    "is_category_missing",
    "is_dims_missing",
    "_silver_processed_at",
]


# Helpers
def _strip_bom_and_whitespace(df: DataFrame) -> DataFrame:
    """Clean header names; the translation CSV header carries a UTF-8 BOM."""
    return df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])


def _check_required_columns(df: DataFrame, required: list, name: str) -> None:
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"{name}: missing required column(s): {sorted(missing)}")


def _clean_str(col_name: str) -> Column:
    """Trim; empty string -> null."""
    trimmed = F.trim(F.col(col_name).cast(StringType()))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def _any(conditions: list) -> Column:
    return reduce(lambda a, b: a | b, conditions)


# Products transformations
def rename_columns(df: DataFrame) -> DataFrame:
    for old, new in RENAMES.items():
        df = df.withColumnRenamed(old, new)
    return df


def cast_types(df: DataFrame) -> DataFrame:
    """Unparseable numbers become null (ANSI off, Fabric default)."""
    for c in INT_COLS:
        df = df.withColumn(c, F.col(c).cast(IntegerType()))
    return df


def standardise_text(df: DataFrame) -> DataFrame:
    return df.withColumn("product_id", _clean_str("product_id")).withColumn(
        "product_category_name", F.lower(_clean_str("product_category_name"))
    )


def drop_exact_duplicates(df: DataFrame) -> DataFrame:
    """Exact duplicates only (e.g. bronze re-appended)."""
    return df.dropDuplicates()


def flag_and_fill(df: DataFrame) -> DataFrame:
    """Flag BEFORE filling so original nullness is preserved."""
    return (
        df.withColumn("is_category_missing", F.col("product_category_name").isNull())
        .withColumn(
            "product_category_name",
            F.coalesce("product_category_name", F.lit(UNKNOWN_CATEGORY)),
        )
        .withColumn("is_dims_missing", _any([F.col(c).isNull() for c in DIM_COLS]))
    )


def add_derived(df: DataFrame) -> DataFrame:
    """Volume as long to rule out int overflow. Null if any size is null."""
    return df.withColumn(
        "product_volume_cm3",
        reduce(lambda a, b: a * b, [F.col(c).cast(LongType()) for c in SIZE_COLS]),
    )


# Translation
def clean_translation(translation: DataFrame) -> DataFrame:
    """Normalise the translation table: trim, lowercase, drop nulls, one row per category."""
    translation = _strip_bom_and_whitespace(translation)
    _check_required_columns(translation, TRANSLATION_COLUMNS, "translation")

    return (
        translation.select(*TRANSLATION_COLUMNS)
        .withColumn(
            "product_category_name", F.lower(_clean_str("product_category_name"))
        )
        .withColumn(
            "product_category_name_english",
            F.lower(_clean_str("product_category_name_english")),
        )
        .dropna(subset=TRANSLATION_COLUMNS)
        .dropDuplicates(["product_category_name"])
    )


def add_english_category(df: DataFrame, translation: DataFrame) -> DataFrame:
    """Unmatched -> "unknown". Validation fails if a NEW category lands here."""
    return df.join(
        F.broadcast(translation), "product_category_name", "left"
    ).withColumn(
        "product_category_name_english",
        F.coalesce("product_category_name_english", F.lit(UNKNOWN_CATEGORY)),
    )


def add_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_silver_processed_at", F.current_timestamp())


# Entry point
def clean_products(products: DataFrame, translation: DataFrame) -> DataFrame:
    """Bronze products + translation -> Silver products."""
    products = _strip_bom_and_whitespace(products)
    _check_required_columns(products, SOURCE_COLUMNS, "products")

    translation_clean = clean_translation(translation)

    return (
        products.select(
            *SOURCE_COLUMNS
        )  # drop bronze metadata so exact-dup check works
        .transform(rename_columns)
        .transform(cast_types)
        .transform(standardise_text)
        .transform(drop_exact_duplicates)
        .transform(flag_and_fill)
        .transform(add_derived)
        .transform(lambda d: add_english_category(d, translation_clean))
        .transform(add_metadata)
        .select(*OUTPUT_COLUMNS)
    )
