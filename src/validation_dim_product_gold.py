"""
Gold-layer validation for the product dimension (dbo.gold_dim_product).

Checks:
    - schema, non-empty, product_id not null / unique
    - 1 row per Silver product
    - baseline_price_med > 0 when present (NULL allowed: product never sold
      outside the event window)
    - English category lowercase with spaces (format_english)
    - English NULL only where native category is NULL or known untranslated
      (catches a broken translation join)

Raises ValueError listing every failed rule with its row count.
"""

from operator import invert

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

try:  # repo / pytest
    from src.cleaning_dim_product_gold import OUTPUT_COLUMNS, format_english
except ModuleNotFoundError:  # Fabric: Files/src on sys.path
    from cleaning_dim_product_gold import OUTPUT_COLUMNS, format_english

# Native categories with no row in the translation table (normalised format).
# English NULL is expected for these; any OTHER untranslated category fails.
KNOWN_UNTRANSLATED = {"pc gamer", "portateis cozinha e preparadores de alimentos"}


def _count_where(condition: Column) -> Column:
    """Rows where condition is TRUE. Null counts as 0; empty df -> 0."""
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))


def validate_dim_product_gold(dim: DataFrame, silver_products: DataFrame) -> DataFrame:
    """Validate gold_dim_product against Silver products. Returns dim unchanged."""
    missing = set(OUTPUT_COLUMNS) - set(dim.columns)
    if missing:
        raise ValueError(f"gold_dim_product missing column(s): {sorted(missing)}")
    missing_silver = {"product_id", "product_category_name"} - set(
        silver_products.columns
    )
    if missing_silver:
        raise ValueError(f"Silver products missing column(s): {sorted(missing_silver)}")

    col = F.col
    native = silver_products.select(
        "product_id", col("product_category_name").alias("_native")
    )
    checked = dim.join(native, "product_id", "left")

    english = col("product_category_name_english")
    rules = {
        "product_id is null": col("product_id").isNull(),
        "baseline_price_med not positive and finite": col(
            "baseline_price_med"
        ).isNotNull()
        & (
            F.isnan("baseline_price_med")
            | (col("baseline_price_med") <= 0)
            | (F.abs(col("baseline_price_med")) == float("inf"))
        ),
        "product_category_name_english not normalised": english.isNotNull()
        & invert(english.eqNullSafe(format_english("product_category_name_english"))),
        "new untranslated category": english.isNull()
        & col("_native").isNotNull()
        & invert(col("_native").isin(*KNOWN_UNTRANSLATED)),
    }
    aggs = [_count_where(cond).alias(name) for name, cond in rules.items()]
    aggs += [
        F.count(F.lit(1)).alias("_total"),
        F.countDistinct("product_id").alias("_distinct_ids"),
        _count_where(col("baseline_price_med").isNull()).alias("_no_baseline"),
        _count_where(english.isNull()).alias("_no_english"),
    ]
    result = checked.agg(*aggs).first().asDict()
    silver_ids = silver_products.select("product_id").distinct().count()

    failures = {name: result[name] for name in rules if result[name] > 0}
    total = result["_total"]
    if total == 0:
        failures["no rows"] = 0
    dup_rows = total - result["product_id is null"] - result["_distinct_ids"]
    if dup_rows > 0:
        failures["duplicate product_id"] = dup_rows
    if total != silver_ids:
        failures[f"row count != Silver distinct product_id ({silver_ids})"] = total

    if failures:
        detail = ", ".join(f"{k} ({v} rows)" for k, v in failures.items())
        raise ValueError(f"gold_dim_product validation failed: {detail}")

    print(
        f"Validation successful: {total} products | "
        f"{result['_no_baseline']} without baseline price | "
        f"{result['_no_english']} without English category"
    )
    return dim
