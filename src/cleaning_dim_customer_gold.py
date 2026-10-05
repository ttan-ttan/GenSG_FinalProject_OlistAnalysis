"""
Gold‑layer cleaning for customers dimension.
Ensures business‑ready fields and removes unusable customer records.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window


def clean_customers_gold(df: DataFrame) -> DataFrame:
    """
    Gold cleaning rules:
    - Ensure customer_id is present
    - Ensure first_purchase_date exists and is not in the future
    - Remove customers with no orders (if business requires)
    - Standardize city/state formatting again
    """

    df_clean = (
        df.filter(F.col("customer_id").isNotNull())
        .filter(F.col("customer_first_purchase_date").isNotNull())
        .filter(F.col("customer_first_purchase_date") <= F.current_timestamp())
        .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    )

    return df_clean


def build_dim_customer(customers: DataFrame, orders: DataFrame) -> DataFrame:
    """Build one keyed customer row per actual person (customer_unique_id)."""
    customer_map = customers.select(
        "customer_id", "customer_unique_id", "customer_city", "customer_state"
    ).dropDuplicates(["customer_id"])
    first_orders = (
        orders.join(
            customer_map.select(
                "customer_id", "customer_unique_id"), "customer_id"
        )
        .groupBy("customer_unique_id")
        .agg(F.min(F.to_date("order_purchase_timestamp")).alias("first_order_date"))
    )
    customer_window = Window.partitionBy(
        "customer_unique_id").orderBy("customer_id")
    customers = (
        customer_map.withColumn("_row", F.row_number().over(customer_window))
        .filter(F.col("_row") == 1)
        .drop("_row", "customer_id")
        .join(first_orders, "customer_unique_id", "left")
        .filter(F.col("customer_unique_id").isNotNull())
        .withColumn("customer_city", F.lower(F.trim("customer_city")))
        .withColumn("state", F.upper(F.trim("customer_state")))
        .drop("customer_state")
    )
    key_window = Window.orderBy("customer_unique_id")
    return (
        customers.withColumn("customer_key", F.row_number().over(key_window))
        .select(
            "customer_key",
            "customer_unique_id",
            "first_order_date",
            "state",
            "customer_city",
        )
        .withColumnRenamed("customer_city", "city")
        .select(
            "customer_key", "customer_unique_id", "first_order_date", "state", "city"
        )
    )


def run_clean(spark):
    """
    Entry point required by Gold Runner Notebook.
    Reads Silver table → applies Gold cleaning → writes Gold table.
    """

    # 1. Read Silver table
    df = spark.read.table("silver_customers")

    orders_df = spark.read.table("silver_orders")
    cleaned_df = build_dim_customer(df, orders_df)

    # 3. Write Gold table
    cleaned_df.write.format("delta").mode(
        "overwrite").saveAsTable("dim_customer")

    print("Gold dimension table 'dim_customer' created successfully.")
