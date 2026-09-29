"""
Silver-layer validation for the products dataset.
Ensures structural and domain correctness before Gold processing.

Allowed by design:
    - product_category_name NULL -> English NULL too.
    - Known untranslated categories -> English NULL.
    - Null dimensions / text metadata (kept, not filled).
    - product_weight_g == 0.
"""

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from src.cleaning_products import OUTPUT_COLUMNS

PRODUCT_ID_PATTERN = r"^[0-9a-f]{32}$"

# Categories with no row in product_category_name_translation.
# English NULL is expected for these; any OTHER untranslated category fails.
KNOWN_UNTRANSLATED = {"pc_gamer", "portateis_cozinha_e_preparadores_de_alimentos"}

NOT_NULL_COLS = ["product_id"]

TEXT_COLS = ["product_category_name", "product_category_name_english"]
DIM_COLS = ["product_length_cm", "product_height_cm", "product_width_cm"]
POSITIVE_COLS = ["product_name_length", "product_description_length", *DIM_COLS]


def _count_where(condition: Column) -> Column:
    """Rows where condition is TRUE. Null counts as 0; empty df -> 0."""
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))


def _row_rules() -> dict:
    """rule name -> condition that marks a BAD row."""
    col = F.col
    rules = {f"{c} is null": col(c).isNull() for c in NOT_NULL_COLS}

    rules["product_id not 32-char lowercase hex"] = ~col("product_id").rlike(
        PRODUCT_ID_PATTERN
    )

    # Standardisation: text must already be trimmed + lowercase, no blanks
    for c in TEXT_COLS:
        rules[f"{c} not normalised"] = col(c) != F.lower(F.trim(col(c)))
        rules[f"{c} is blank"] = col(c) == ""

    # English NULL only for missing or known-untranslated categories
    rules["new untranslated category"] = (
        col("product_category_name_english").isNull()
        & col("product_category_name").isNotNull()
        & ~col("product_category_name").isin(*KNOWN_UNTRANSLATED)
    )
    rules["english category without native category"] = (
        col("product_category_name").isNull()
        & col("product_category_name_english").isNotNull()
    )

    # Basic integrity: weight 0 allowed, negative not
    rules["product_weight_g negative"] = col("product_weight_g") < 0
    for c in POSITIVE_COLS:
        rules[f"{c} not positive"] = col(c) <= 0
    rules["product_photos_qty < 1"] = col("product_photos_qty") < 1

    return rules


def validate_products(df: DataFrame) -> DataFrame:
    """Validate Silver products. Returns df unchanged or raises ValueError."""

    # Schema check first; row rules would fail on missing columns
    missing = set(OUTPUT_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"silver_products missing column(s): {sorted(missing)}")

    rules = _row_rules()
    aggs = [_count_where(cond).alias(name) for name, cond in rules.items()]
    aggs += [
        F.count(F.lit(1)).alias("_total"),
        F.countDistinct("product_id").alias("_distinct_ids"),
    ]
    result = df.agg(*aggs).first().asDict()

    failures = {name: result[name] for name in rules if result[name] > 0}

    # Address conflicting duplicates
    non_null_ids = result["_total"] - result["product_id is null"]
    dup_rows = non_null_ids - result["_distinct_ids"]
    if dup_rows > 0:
        failures["duplicate product_id"] = dup_rows

    if failures:
        detail = ", ".join(f"{k} ({v} rows)" for k, v in failures.items())
        raise ValueError(f"silver_products validation failed: {detail}")

    return df
