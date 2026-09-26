"""
Silver-layer validation for the products dataset.
Ensures structural and domain correctness before Gold processing.

All row-level rules are counted in ONE aggregation pass. Every failing rule
is reported together in a single ValueError.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from src.cleaning_products import KNOWN_UNTRANSLATED, OUTPUT_COLUMNS, UNKNOWN_CATEGORY

PRODUCT_ID_PATTERN = r"^[0-9a-f]{32}$"

NOT_NULL_COLS = [
    "product_id",
    "product_category_name",
    "product_category_name_english",
    "is_category_missing",
    "is_dims_missing",
]


def _count_where(condition) -> F.Column:
    """Rows where condition is TRUE. Null counts as 0; empty df -> 0."""
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))


def _row_rules() -> dict:
    """rule name -> condition that marks a BAD row."""
    col = F.col
    rules = {f"{c} is null": col(c).isNull() for c in NOT_NULL_COLS}
    rules.update(
        {
            "product_id not 32-char lowercase hex": ~col("product_id").rlike(
                PRODUCT_ID_PATTERN
            ),
            # "unknown" english allowed only for missing or known-untranslated categories
            "new untranslated category": (
                col("product_category_name_english") == UNKNOWN_CATEGORY
            )
            & ~col("is_category_missing")
            & ~col("product_category_name").isin(*KNOWN_UNTRANSLATED),
            "is_category_missing inconsistent": col("is_category_missing")
            & (col("product_category_name") != UNKNOWN_CATEGORY),
            "is_dims_missing inconsistent": col("is_dims_missing")
            != (
                col("product_weight_g").isNull()
                | col("product_length_cm").isNull()
                | col("product_height_cm").isNull()
                | col("product_width_cm").isNull()
            ),
            # weight 0 allowed, negative not
            "product_weight_g negative": col("product_weight_g") < 0,
            "product_length_cm not positive": col("product_length_cm") <= 0,
            "product_height_cm not positive": col("product_height_cm") <= 0,
            "product_width_cm not positive": col("product_width_cm") <= 0,
            "product_volume_cm3 mismatch": col("product_volume_cm3")
            != (
                col("product_length_cm").cast("long")
                * col("product_height_cm").cast("long")
                * col("product_width_cm").cast("long")
            ),
            "product_photos_qty < 1": col("product_photos_qty") < 1,
            "product_name_length not positive": col("product_name_length") <= 0,
            "product_description_length not positive": col("product_description_length")
            <= 0,
        }
    )
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

    # conflicting duplicates survive cleaning on purpose; catch them here
    non_null_ids = result["_total"] - result["product_id is null"]
    dup_rows = non_null_ids - result["_distinct_ids"]
    if dup_rows > 0:
        failures["duplicate product_id"] = dup_rows

    if failures:
        detail = ", ".join(f"{k} ({v} rows)" for k, v in failures.items())
        raise ValueError(f"silver_products validation failed: {detail}")

    return df
