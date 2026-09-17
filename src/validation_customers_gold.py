# pylint: disable=no-member
# pylint: disable=invalid-unary-operand-type

"""
Gold-layer validation for customers dimension.
Ensures dimensional integrity and business rule correctness.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

VALID_STATES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG",
    "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
}


def validate_customers_gold(df: DataFrame) -> DataFrame:
    """Validate Gold customer dimension."""

    # Unique customer_id
    dup_ids = (
        df.groupBy("customer_id")
          .count()
          .filter(F.col("count") > 1)
    )
    if dup_ids.count() > 0:
        raise ValueError(
            f"Duplicate customer_id in Gold: {dup_ids.first()['customer_id']}")

    # Valid state codes
    invalid_states = df.filter(
        ~F.col("customer_state").isin(list(VALID_STATES)))
    if invalid_states.count() > 0:
        raise ValueError(
            f"Invalid state in Gold: {invalid_states.first()['customer_state']}")

    # Logical first_purchase_date
    future_dates = df.filter(
        F.col("customer_first_purchase_date") > F.current_timestamp())
    if future_dates.count() > 0:
        raise ValueError("Gold contains future first_purchase_date")

    # Null critical fields
    critical_cols = [
        "customer_id",
        "customer_city",
        "customer_state",
        "customer_first_purchase_date"
    ]
    for col in critical_cols:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in Gold: {col}")

    return df
