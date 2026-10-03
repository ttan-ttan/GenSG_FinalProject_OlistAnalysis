# pylint: disable=no-member
# pylint: disable=invalid-unary-operand-type

"""
Gold-layer validation for customers dimension.
Ensures dimensional integrity and business rule correctness.
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
    "customer_key",
    "customer_unique_id",
    "first_order_date",
    "state",
    "city",
]


def validate_dim_customer_gold(df: DataFrame) -> DataFrame:
    """Validate Gold customer dimension."""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in Gold customers: {missing}")

    # Null critical fields
    for col in REQUIRED_COLUMNS:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in Gold: {col}")

    for key in ["customer_key", "customer_unique_id"]:
        dup_ids = df.groupBy(key).count().filter(F.col("count") > 1)
        if dup_ids.count() > 0:
            raise ValueError(f"Duplicate {key} in Gold customer dimension")

    # Valid state codes
    invalid_states = df.filter(invert(F.col("state").isin(list(VALID_STATES))))
    if invalid_states.count() > 0:
        raise ValueError(f"Invalid state in Gold: {invalid_states.first()['state']}")

    invalid_cities = df.filter(F.col("city") != F.lower(F.trim(F.col("city"))))
    if invalid_cities.count() > 0:
        raise ValueError("city is not normalized in Gold")

    parsed_purchase_date = F.try_to_timestamp(F.col("first_order_date"))
    invalid_dates = df.filter(parsed_purchase_date.isNull())
    if invalid_dates.count() > 0:
        raise ValueError("Invalid first_order_date in Gold")

    # Logical first_purchase_date
    future_dates = df.filter(parsed_purchase_date > F.current_timestamp())
    if future_dates.count() > 0:
        raise ValueError("Gold contains future first_order_date")

    return df


validate_customers_gold = validate_dim_customer_gold
