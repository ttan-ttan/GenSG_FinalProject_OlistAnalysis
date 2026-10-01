# pylint: disable=E1101
"""
Centralized DQ logging script for Fabric pipeline.
"""

from pyspark.sql import SparkSession


def log(stage, dataset, count):
    spark = SparkSession.builder.getOrCreate()
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS dq_rowcount_logs (
            dataset STRING,
            stage STRING,
            row_count BIGINT,
            log_timestamp TIMESTAMP
        )
    """
    )
    spark.sql(
        f"""
        INSERT INTO dq_rowcount_logs
        VALUES ('{dataset}', '{stage}', {count}, current_timestamp())
    """
    )
    print(f"[ROWCOUNT] {dataset} | {stage} | {count}")


def log_invalid(dataset):
    spark = SparkSession.builder.getOrCreate()
    invalid_df = spark.sql("SELECT * FROM all_invalid")
    count = invalid_df.count()

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS dq_invalid_logs (
            dataset STRING,
            invalid_rows BIGINT,
            log_timestamp TIMESTAMP
        )
    """
    )

    spark.sql(
        f"""
        INSERT INTO dq_invalid_logs
        VALUES ('{dataset}', {count}, current_timestamp())
    """
    )

    print(f"[INVALID] {dataset} | invalid_rows = {count}")
