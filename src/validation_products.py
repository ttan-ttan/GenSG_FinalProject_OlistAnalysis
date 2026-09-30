"""
Silver-layer validation for the products dataset.
Ensures structural and domain correctness before Gold processing.

Translation checks (untranslated categories) live in Gold:
validation_dim_product_gold.

Allowed by design:
    - product_category_name NULL.
    - Null dimensions / text metadata (kept, not filled).
    - product_weight_g == 0.
"""

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

try:  # repo / pytest
    from src.cleaning_products import OUTPUT_COLUMNS, normalise_category
except ModuleNotFoundError:  # Fabric: Files/src on sys.path
    from cleaning_products import OUTPUT_COLUMNS, normalise_category

PRODUCT_ID_PATTERN = r"^[0-9a-f]{32}$"

NOT_NULL_COLS = ["product_id"]

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

    # Category must already be in the shared format (matches translation key).
    # Blank strings also fail here: normalise_category turns them into NULL.
    rules["product_category_name not normalised"] = col(
        "product_category_name"
    ).isNotNull() & ~col("product_category_name").eqNullSafe(
        normalise_category("product_category_name")
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
