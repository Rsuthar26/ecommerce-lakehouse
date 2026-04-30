# ============================================================
# bronze_utils.py — Shared utilities for all Bronze notebooks
#
# Every Bronze source notebook imports from here.
# Never reimplement these functions per notebook.
#
# Functions:
#   get_watermark()    — read last processed timestamp for a source
#   update_watermark() — write new watermark after successful load
#
# Watermark table: bronze.pipeline.watermarks
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime


def get_watermark(spark: SparkSession, source: str) -> datetime:
    """
    Returns the last processed file timestamp for a source.
    If no watermark exists (first run), returns epoch (1970-01-01).
    This ensures first run loads all historical data.
    """
    result = spark.sql(f"""
        SELECT COALESCE(
            MAX(last_file_ts),
            CAST('1970-01-01 00:00:00' AS TIMESTAMP)
        ) AS last_file_ts
        FROM bronze.pipeline.watermarks
        WHERE source = '{source}'
    """).collect()[0]['last_file_ts']

    print(f"[{source}] Watermark: {result}")
    return result


def update_watermark(
    spark: SparkSession,
    source: str,
    last_file_ts,
    rows_loaded: int
) -> None:
    """
    Writes a new watermark entry after a successful load.
    Append-only — never updates existing rows.
    Full history is preserved for auditing.
    """
    spark.sql(f"""
        INSERT INTO bronze.pipeline.watermarks
        VALUES (
            '{source}',
            current_timestamp(),
            CAST('{last_file_ts}' AS TIMESTAMP),
            {rows_loaded}
        )
    """)
    print(f"[{source}] Watermark updated — last_file_ts={last_file_ts}, rows_loaded={rows_loaded}")
