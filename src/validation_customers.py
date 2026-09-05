""" 
Customer Validation Module 
- validates the cleaned customers dataset before moving to the Gold layer.
- Validation:
        1. Ensure customer_state is a valid Brazilian state code
        2. Ensure zip code prefix is within valid range (1000-99999)
        3. Ensure customer_id is unique (no duplicates allowed)
- If validation fails, descriptive errors are raised and process stopped until rectified. 
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def validate_customers(df: DataFrame) -> DataFrame:
    """
    Validate the customers dataset loaded from silver (data/cleaned/customers.csv) 
    and return as validated dataframe ready for Gold layer.
    Raise ValueError: If duplicate customer_id values are found.
    """
    # Define valid Brazilian state codes
    valid_states = [
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
        "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
        "RS", "RO", "RR", "SC", "SP", "SE", "TO"
    ]

    # check customer_state is valid and zip code prefix is within valid range
    # there is no zipcode lower than 1000 in Brazil, and the maximum is 99999
    df_val = (
        df
        .filter(F.col("customer_state").isin(valid_states))
        .filter(F.col("customer_zip_code_prefix").between(1000, 99999))
    )

    # Check uniqueness of customer_id
    duplicates = (
        df_val.groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
    )
    # If duplicates are found, raise a ValueError with a descriptive message
    if duplicates.count() > 0:
        raise ValueError("Duplicate customer_id found in customers dataset")

    return df_val
