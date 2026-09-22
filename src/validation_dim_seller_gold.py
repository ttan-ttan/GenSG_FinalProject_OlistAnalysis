# pylint: disable=no-member

"""
Gold Validation: Seller Dimension
Purpose:
    Validate dimensional integrity + business rules.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

VALID_STATES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG",
    "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
}


def validate_dim_seller_gold(df: DataFrame) -> DataFrame:
    """Validate Gold seller dimension."""

    # Unique seller_id
    dup = df.groupBy("seller_id").count().filter(F.col("count") > 1)
    if dup.count() > 0:
        raise ValueError(
            f"Duplicate seller_id in Gold: {dup.first()['seller_id']}")

    # Valid state codes
    invalid = df.filter(
        F.col("seller_state").isin(list(VALID_STATES)).__invert__()
    )
    if invalid.count() > 0:
        raise ValueError(
            f"Invalid seller_state in Gold: {invalid.first()['seller_state']}")

    # Null critical fields
    critical = ["seller_id", "seller_city",
                "seller_state", "seller_zip_code_prefix"]
    for col in critical:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in Gold: {col}")

    return df
