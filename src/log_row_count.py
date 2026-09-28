# pylint: disable=E1101
"""
Utility module for logging row counts into dq_rowcount_logs.
"""

from pyspark.sql import SparkSession


def log_rowcount(dataset: str, stage: str, count: int):
    spark = SparkSession.builder.getOrCreate()

    spark.sql("""
        CREATE TABLE IF NOT EXISTS dq_rowcount_logs (
            dataset STRING,
            stage STRING,
            row_count BIGINT,
            log_timestamp TIMESTAMP
        )
    """)

    spark.sql(f"""
        INSERT INTO dq_rowcount_logs
        VALUES ('{dataset}', '{stage}', {count}, current_timestamp())
    """)

    print(f"[ROWCOUNT LOG] {dataset} | {stage} | {count}")
