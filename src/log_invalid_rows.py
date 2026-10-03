# pylint: disable=E1101
"""
Utility module for logging detailed invalid row records into dbo.dq_invalid_row_logs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


def log_invalid_rows(dataset: str, invalid_df):
    """
    Logs granular invalid rows into dbo.dq_invalid_row_logs table.

    Parameters:
        dataset (str): dataset name, e.g. 'customers'
        invalid_df (DataFrame): dataframe containing invalid rows
    """
    spark = SparkSession.builder.getOrCreate()

    # 1. Ensure table exists in dbo schema
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS dbo.dq_invalid_row_logs (
            dataset STRING,
            column_name STRING,
            rule STRING,
            invalid_value STRING,
            log_timestamp TIMESTAMP
        )
        """
    )

    # 2. Append detailed invalid rows using native Column expressions
    log_df = invalid_df.withColumn("dataset", lit(dataset)) \
                       .withColumn("log_timestamp", current_timestamp())

    log_df.write.format("delta").mode(
        "append").saveAsTable("dbo.dq_invalid_row_logs")

    print(
        f"[DETAILED INVALID LOG] {dataset} → {invalid_df.count()} invalid rows logged.")
