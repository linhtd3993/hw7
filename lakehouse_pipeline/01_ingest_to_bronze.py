"""
Pipeline nạp dữ liệu thô vào tầng Bronze.

Sử dụng Spark Structured Streaming để giám sát thư mục ``local_landing_zone/``,
tự động nạp các file CSV mới phát sinh, thêm metadata kỹ thuật (ingest_timestamp,
source_file), chuẩn hóa tên cột, và ghi xuống tầng Bronze dưới định dạng Delta Lake.
"""
import os

from pyspark.sql.functions import current_timestamp, input_file_name

from lakehouse_pipeline.config import (
    get_spark_session,
    get_storage_paths,
    clean_dataframe_columns,
    STREAMING_TIMEOUT_SECONDS,
)
from lakehouse_pipeline.logger import get_logger

logger = get_logger("bronze_ingestion")


def ingest_to_bronze() -> None:
    """Nạp dữ liệu thô từ Landing Zone vào tầng Bronze trên Azure/Local.

    Quy trình:
        1. Khởi tạo Spark Structured Streaming để đọc CSV từ landing zone.
        2. Chuẩn hóa toàn bộ tên cột sang snake_case (Delta Lake yêu cầu).
        3. Thêm cột ``ingest_timestamp`` và ``source_file`` để truy vết.
        4. Ghi stream xuống Bronze ở định dạng Delta với checkpoint.

    Raises:
        pyspark.sql.utils.StreamingQueryException: Nếu streaming query gặp lỗi.
    """
    logger.info("=== BẮT ĐẦU PIPELINE NẠP DỮ LIỆU TẦNG BRONZE ===")

    spark = get_spark_session("BronzeIngestion")
    paths = get_storage_paths()

    # Kích hoạt suy luận schema tự động cho Structured Streaming
    spark.conf.set("spark.sql.streaming.schemaInference", "true")

    landing_zone = paths["landing_zone"]
    bronze_path = paths["bronze"]
    checkpoint_path = os.path.join(bronze_path, "_checkpoint")

    logger.info("Landing Zone : %s", landing_zone)
    logger.info("Bronze path  : %s", bronze_path)
    logger.info("Checkpoint   : %s", checkpoint_path)

    # 1. Đọc CSV stream từ landing zone
    df_stream = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(landing_zone)
    )

    # 2. Chuẩn hóa tên cột (dùng hàm tiện ích chung từ config)
    df_stream = clean_dataframe_columns(df_stream)

    # 3. Thêm metadata kỹ thuật cho data lineage
    df_bronze = (
        df_stream
        .withColumn("ingest_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    # 4. Ghi stream xuống Bronze ở định dạng Delta Lake
    logger.info("Khởi chạy Structured Streaming Query...")
    query = (
        df_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(bronze_path)
    )

    logger.info("Đang nạp dữ liệu (timeout=%ds)...", STREAMING_TIMEOUT_SECONDS)

    try:
        query.awaitTermination(timeout=STREAMING_TIMEOUT_SECONDS)
        logger.info("Đã hoàn tất nạp các lô dữ liệu hiện tại.")
    except Exception:
        logger.exception("Lỗi xảy ra trong quá trình streaming")
    finally:
        query.stop()
        spark.stop()
        logger.info("=== PIPELINE BRONZE ĐÃ HOÀN THÀNH ===")


if __name__ == "__main__":
    ingest_to_bronze()
