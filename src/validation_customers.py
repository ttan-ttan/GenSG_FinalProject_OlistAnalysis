# pylint: disable=invalid-unary-operand-type
# pylint: disable=no-member

"""
Silver-layer validation for the customers dataset.
Ensures structural and domain correctness before Gold processing.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

VALID_STATES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG",
    "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
}


def validate_customers(df: DataFrame) -> DataFrame:
    """Validate customer records for Silver layer."""

    # Critical null checks
    critical_cols = ["customer_id", "customer_unique_id",
                     "customer_city", "customer_state"]
    for col in critical_cols:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null value found in critical field: {col}")

    # Valid state codes
    invalid_states = df.filter(
        ~F.col("customer_state").isin(list(VALID_STATES)))
    if invalid_states.count() > 0:
        raise ValueError(
            f"Invalid customer_state: {invalid_states.first()['customer_state']}")

    # Duplicate customer_id
    dup_ids = (
        df.groupBy("customer_id")
          .count()
          .filter(F.col("count") > 1)
    )
    if dup_ids.count() > 0:
        raise ValueError(
            f"Duplicate customer_id: {dup_ids.first()['customer_id']}")

    # ZIP code range
    invalid_zip = df.filter(
        (F.col("customer_zip_code_prefix") < 1000) |
        (F.col("customer_zip_code_prefix") > 99999)
    )
    if invalid_zip.count() > 0:
        raise ValueError(
            f"Invalid ZIP code prefix: {invalid_zip.first()['customer_zip_code_prefix']}")

    return df
