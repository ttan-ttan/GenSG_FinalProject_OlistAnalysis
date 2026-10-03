""" cleaning_dim_date_gold
This module contains functions to clean and build the Gold date dimension table."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ORDER_DATE_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
BLACK_FRIDAY = "2017-11-24"
EVENT_START = "2017-11-24"
EVENT_END = "2017-11-26"


def build_dim_date(
    df: DataFrame, timestamp_col: str = "order_purchase_timestamp"
) -> DataFrame:
    """Build a keyed calendar covering all available order-date roles."""
    requested_columns = (
        [timestamp_col]
        if timestamp_col != "order_purchase_timestamp"
        else ORDER_DATE_COLUMNS
    )
    date_columns = [column for column in requested_columns if column in df.columns]
    if not date_columns:
        raise ValueError(f"No order date columns found among: {requested_columns}")

    date_values = df.select(F.to_date(F.col(date_columns[0])).alias("date"))
    for column in date_columns[1:]:
        date_values = date_values.unionByName(
            df.select(F.to_date(F.col(column)).alias("date"))
        )
    date_values = date_values.dropna().distinct()
    date_bounds = date_values.agg(
        F.min("date").alias("start_date"),
        F.max("date").alias("end_date"),
    )
    date_df = date_bounds.select(
        F.explode(
            F.sequence(
                F.col("start_date"),
                F.col("end_date"),
                F.expr("INTERVAL 1 DAY"),
            )
        ).alias("date")
    )

    return (
        date_df.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("dow", F.dayofweek("date"))
        .withColumn("is_black_friday", F.col("date") == F.to_date(F.lit(BLACK_FRIDAY)))
        .withColumn(
            "event_window",
            F.col("date").between(
                F.to_date(F.lit(EVENT_START)), F.to_date(F.lit(EVENT_END))
            ),
        )
        .withColumn(
            "is_weekend",
            F.dayofweek("date").isin(1, 7),
        )
        .select(
            "date_key",
            "date",
            "year",
            "month",
            "dow",
            "is_black_friday",
            "event_window",
            "is_weekend",
        )
    )


def clean_dim_date_gold(
    df: DataFrame, timestamp_col: str = "order_purchase_timestamp"
) -> DataFrame:
    """Clean date dimension inputs by removing null timestamps and standardizing output fields."""
    return build_dim_date(df, timestamp_col)


def run_clean(spark) -> DataFrame:
    """Build and write the Gold date dimension from the Silver orders table."""
    orders_df = spark.read.table("orders_silver")
    gold_df = clean_dim_date_gold(orders_df)
    gold_df.write.format("delta").mode("overwrite").saveAsTable("dim_date")
    print("Gold dimension table 'dim_date' created successfully.")
    return gold_df


# Common alias used by the project naming pattern.
clean_date_gold = clean_dim_date_gold
