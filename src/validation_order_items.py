# pylint: disable=no-member

"""
Validation rules for the Silver Order Items dataset.

Purpose:
    Check that cleaned order-item data is complete, consistent,
    and suitable for downstream analysis.

Validation handled here:
    1. Ensure all required columns exist.
    2. Ensure key identifiers are not null.
    3. Ensure each order-item combination is unique.
    4. Ensure order_item_id is valid.
    5. Ensure price and freight values are reasonable.
    6. Ensure shipping_limit_date is present.

Invalid values are not silently corrected because doing so could
introduce assumptions into the dataset.
"""

import pyspark.sql.functions as F


def validate_order_items(df):
    """
    Validating the Order Items DataFrame.

    Parameters:
        df: PySpark DataFrame containing cleaned order-item records.

    Returns:
        The validated PySpark DataFrame.

    Raises:
        ValueError: If any validation rule fails.
    """

    required_columns = [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ]

    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Scenario 1:
    # Missing identifiers prevent an order item from being linked
    # correctly to its order, product, or seller.
    id_columns = [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
    ]

    for column in id_columns:
        if df.filter(F.col(column).isNull()).count() > 0:
            raise ValueError(f"{column} contains null values")

    # order_id alone is not unique because one order can contain
    # multiple items. The combination of order_id and order_item_id
    # should therefore be unique.
    dup_count = (
        df.groupBy("order_id", "order_item_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if dup_count > 0:
        raise ValueError(
            "Duplicate order_id and order_item_id combination detected"
        )

    if df.filter(F.col("order_item_id") <= 0).count() > 0:
        raise ValueError("Invalid order_item_id detected")

    # Scenario 2:
    # Zero or negative prices would distort sales and revenue analysis.
    if df.filter(
        F.col("price").isNull() | (F.col("price") <= 0)
    ).count() > 0:
        raise ValueError("Invalid price detected")

    # Freight cannot be negative, although zero freight may be valid.
    if df.filter(
        F.col("freight_value").isNull() | (F.col("freight_value") < 0)
    ).count() > 0:
        raise ValueError("Invalid freight_value detected")

    if df.filter(F.col("shipping_limit_date").isNull()).count() > 0:
        raise ValueError("shipping_limit_date contains null values")

    return df