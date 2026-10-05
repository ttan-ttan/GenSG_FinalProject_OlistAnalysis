# pylint: disable=no-member

"""
validation_dim_seller_gold
Gold Validation: Seller Dimension
Ensures dimensional integrity + business rules.
"""

from operator import invert
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

VALID_STATES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG",
    "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
}

REQUIRED_COLUMNS = [
    "seller_key",
    "seller_id",
    "seller_state",
    "seller_city",
]


def validate_dim_seller_gold(df: DataFrame) -> DataFrame:
    """Validate Gold seller dimension."""
    # Schema check
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns in gold_dim_seller: {missing}")

    # Duplicate checks
    for key in ["seller_key", "seller_id"]:
        dup = df.groupBy(key).count().filter(F.col("count") > 1)
        if dup.count() > 0:
            raise ValueError(f"Duplicate {key} detected in gold_dim_seller")

    # Null checks
    for col in REQUIRED_COLUMNS:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in gold_dim_seller: {col}")

    # Valid state codes
    invalid_states = df.filter(
        invert(F.col("seller_state").isin(list(VALID_STATES))))
    if invalid_states.count() > 0:
        raise ValueError(
            f"Invalid seller_state detected: {invalid_states.first()['seller_state']}"
        )

    # Normalized city check
    invalid_city = df.filter(
        F.col("seller_city") != F.lower(F.trim(F.col("seller_city")))
    )
    if invalid_city.count() > 0:
        raise ValueError("seller_city is not normalized (lowercase + trimmed)")

    return df


validate_seller_gold = validate_dim_seller_gold
