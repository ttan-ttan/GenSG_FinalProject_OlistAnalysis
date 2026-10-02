""" cleaning_dim_date_gold
This module contains functions to clean and build the Gold date dimension table."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_date(
    df: DataFrame, timestamp_col: str = "order_purchase_timestamp"
) -> DataFrame:
    """Build a date dimension from a timestamp column."""
    date_values = df.select(
        F.to_date(F.col(timestamp_col)).alias("calendar_date")
    ).dropna()
    date_bounds = date_values.agg(
        F.min("calendar_date").alias("start_date"),
        F.max("calendar_date").alias("end_date"),
    )
    date_df = date_bounds.select(
        F.explode(
            F.sequence(
                F.col("start_date"),
                F.col("end_date"),
                F.expr("INTERVAL 1 DAY"),
            )
        ).alias("calendar_date")
    )

    return (
        date_df.withColumn("year_num", F.year("calendar_date"))
        .withColumn("month_num", F.month("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("day_num", F.dayofmonth("calendar_date"))
        .withColumn("day_of_week_num", F.dayofweek("calendar_date"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("calendar_date").isin(1, 7), F.lit(True)).otherwise(
                F.lit(False)
            ),
        )
    )


def clean_dim_date_gold(
    df: DataFrame, timestamp_col: str = "order_purchase_timestamp"
) -> DataFrame:
    """Clean date dimension inputs by removing null timestamps and standardizing output fields."""
    return build_dim_date(df, timestamp_col)


# Common alias used by the project naming pattern.
clean_date_gold = clean_dim_date_gold
