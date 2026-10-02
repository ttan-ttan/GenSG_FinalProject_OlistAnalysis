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
    "customer_id",
    "customer_city",
    "customer_state",
    "customer_first_purchase_date",
]


def validate_dim_customer_gold(df: DataFrame) -> DataFrame:
    """Validate Gold customer dimension."""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns in Gold customers: {missing}")

    # Null critical fields
    for col in REQUIRED_COLUMNS:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in Gold: {col}")

    # Unique customer_id
    dup_ids = df.groupBy("customer_id").count().filter(F.col("count") > 1)
    if dup_ids.count() > 0:
        raise ValueError(
            f"Duplicate customer_id in Gold: {dup_ids.first()['customer_id']}"
        )

    # Valid state codes
    invalid_states = df.filter(
        invert(F.col("customer_state").isin(list(VALID_STATES)))
    )
    if invalid_states.count() > 0:
        raise ValueError(
            f"Invalid state in Gold: {invalid_states.first()['customer_state']}"
        )

    invalid_cities = df.filter(
        F.col("customer_city") != F.lower(F.trim(F.col("customer_city")))
    )
    if invalid_cities.count() > 0:
        raise ValueError("customer_city is not normalized in Gold")

    parsed_purchase_date = F.to_timestamp(
        F.col("customer_first_purchase_date"))
    invalid_dates = df.filter(parsed_purchase_date.isNull())
    if invalid_dates.count() > 0:
        raise ValueError("Invalid customer_first_purchase_date in Gold")

    # Logical first_purchase_date
    future_dates = df.filter(parsed_purchase_date > F.current_timestamp())
    if future_dates.count() > 0:
        raise ValueError("Gold contains future first_purchase_date")

    return df


validate_customers_gold = validate_dim_customer_gold
