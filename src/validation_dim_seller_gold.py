# pylint: disable=no-member

"""
Gold Validation: Seller Dimension
Purpose:
    Validate dimensional integrity + business rules.
"""

from operator import invert

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

VALID_STATES = {
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
}

REQUIRED_COLUMNS = [
    "seller_key",
    "seller_id",
    "seller_state",
    "seller_city",
]


def validate_dim_seller_gold(df: DataFrame) -> DataFrame:
    """Validate Gold seller dimension."""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in Gold sellers: {missing}")

    for key in ["seller_key", "seller_id"]:
        dup = df.groupBy(key).count().filter(F.col("count") > 1)
        if dup.count() > 0:
            raise ValueError(f"Duplicate {key} in Gold seller dimension")

    # Valid state codes
    invalid = df.filter(invert(F.col("seller_state").isin(list(VALID_STATES))))
    if invalid.count() > 0:
        raise ValueError(
            f"Invalid seller_state in Gold: {invalid.first()['seller_state']}"
        )

    invalid_cities = df.filter(
        F.col("seller_city") != F.lower(F.trim(F.col("seller_city")))
    )
    if invalid_cities.count() > 0:
        raise ValueError("seller_city is not normalized in Gold")

    # Null critical fields
    critical = ["seller_key", "seller_id", "seller_city", "seller_state"]
    for col in critical:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in Gold: {col}")

    return df
