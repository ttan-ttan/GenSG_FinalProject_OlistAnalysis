# pylint: disable=invalid-unary-operand-type
# pylint: disable=no-member

"""
Validation logic for the customers dataset.
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


VALID_STATES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
}


def validate_customers(df: DataFrame) -> DataFrame:
    """
    Validate customer records:
    - customer_state must be a valid 2‑letter Brazilian state code
    - customer_id must be unique
    - ZIP code prefix must be between 1000 and 99999
    """

    # 1. Validate state codes
    invalid_states = (
        df.filter(~F.col("customer_state").isin(list(VALID_STATES)))
    )

    if invalid_states.count() > 0:
        bad_state = invalid_states.first()["customer_state"]
        raise ValueError(f"Invalid customer_state: {bad_state}")

    # 2. Validate duplicate customer_id
    dup_ids = (
        df.groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
    )

    if dup_ids.count() > 0:
        bad_id = dup_ids.first()["customer_id"]
        raise ValueError(f"Duplicate customer_id: {bad_id}")

    # 3. Validate ZIP code range
    invalid_zip = df.filter(
        (F.col("customer_zip_code_prefix") < 1000)
        | (F.col("customer_zip_code_prefix") > 99999)
    )

    if invalid_zip.count() > 0:
        bad_zip = invalid_zip.first()["customer_zip_code_prefix"]
        raise ValueError(f"Invalid ZIP code prefix: {bad_zip}")

    return df
