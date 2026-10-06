# pylint: disable=no-member

"""
cleaning_dim_date_gold
Gold-layer build for the date dimension.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def build_dim_date_gold(orders: DataFrame) -> DataFrame:
    """Build date dimension from silver_orders."""
    df = (
        orders.select(F.to_date("order_purchase_timestamp").alias("date"))
              .dropDuplicates()
              .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
              .withColumn("year", F.year("date"))
              .withColumn("month", F.month("date"))
              .withColumn("day", F.dayofmonth("date"))
              .withColumn("weekday", F.date_format("date", "EEEE"))
    )
    return df.select("date_key", "date", "year", "month", "day", "weekday")


def run_clean(spark):
    """Run Gold date dimension build."""
    orders = spark.read.table("dbo.silver_orders")
    gold_df = build_dim_date_gold(orders)

    # Overwrite Gold table
    gold_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("dbo.gold_dim_date")

    print("Gold dimension table 'dbo.gold_dim_date' created successfully.")
    return gold_df
