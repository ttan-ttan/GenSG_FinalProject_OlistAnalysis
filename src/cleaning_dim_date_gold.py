"""
cleaning_dim_date_gold
Gold-layer build for the date dimension.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def build_dim_date_gold(orders: DataFrame) -> DataFrame:
    df = (
        orders.select(F.to_date("order_purchase_timestamp").alias("date"))
              .dropDuplicates()
              .withColumn("date_key", F.date_format("date", "yyyyMMdd"))
              .withColumn("year", F.year("date"))
              .withColumn("month", F.month("date"))
              .withColumn("day", F.dayofmonth("date"))
              .withColumn("weekday", F.date_format("date", "EEEE"))
    )
    return df.select("date_key", "date", "year", "month", "day", "weekday")


def run_clean(spark):
    orders = spark.read.table("silver_orders")
    gold_df = build_dim_date_gold(orders)

    gold_df.write.format("delta").mode(
        "overwrite").saveAsTable("gold_dim_date")
    print("Gold dimension table 'gold_dim_date' created successfully.")
    return gold_df
