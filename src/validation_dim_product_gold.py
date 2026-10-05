"""
validation_dim_product_gold
Gold-layer validation for the product dimension (gold_dim_product).

Checks:
    - schema, non-empty, product_id not null / unique
    - 1 row per Silver product
    - baseline_price_med > 0 when present (NULL allowed)
    - English category normalized (lowercase, spaces)
    - English NULL only for known untranslated categories
"""

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from operator import invert

try:
    from cleaning_dim_product_gold import OUTPUT_COLUMNS, format_english
except ModuleNotFoundError:
    from src.cleaning_dim_product_gold import OUTPUT_COLUMNS, format_english

KNOWN_UNTRANSLATED = {
    "pc gamer",
    "portateis cozinha e preparadores de alimentos",
}


def _count_where(condition: Column) -> Column:
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))


def validate_dim_product_gold(dim: DataFrame, silver_products: DataFrame) -> DataFrame:
    """Validate gold_dim_product against Silver products."""
    # Schema check
    missing = set(OUTPUT_COLUMNS) - set(dim.columns)
    if missing:
        raise ValueError(
            f"gold_dim_product missing column(s): {sorted(missing)}")

    missing_silver = {"product_id", "product_category_name"} - \
        set(silver_products.columns)
    if missing_silver:
        raise ValueError(
            f"Silver products missing column(s): {sorted(missing_silver)}")

    native = silver_products.select(
        "product_id", F.col("product_category_name").alias("_native")
    )
    checked = dim.join(native, "product_id", "left")

    english = F.col("category_en")
    baseline = F.col("baseline_price_med")

    rules = {
        "product_key is null": F.col("product_key").isNull(),
        "product_id is null": F.col("product_id").isNull(),
        "baseline_price_med invalid": baseline.isNotNull() & (
            F.isnan("baseline_price_med")
            | (baseline <= 0)
            | (F.abs(baseline) == float("inf"))
        ),
        "category_en not normalized": english.isNotNull()
        & invert(english.eqNullSafe(format_english("category_en"))),
        "unexpected untranslated category": english.isNull()
        & F.col("_native").isNotNull()
        & invert(F.col("_native").isin(*KNOWN_UNTRANSLATED)),
    }

    aggs = [_count_where(cond).alias(name) for name, cond in rules.items()]
    aggs += [
        F.count(F.lit(1)).alias("_total"),
        F.countDistinct("product_id").alias("_distinct_ids"),
        _count_where(baseline.isNull()).alias("_no_baseline"),
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


validate_product_gold = validate_dim_product_gold
