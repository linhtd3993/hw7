"""
Script kiểm tra và hiển thị thông tin bảng Delta tầng Bronze.
"""
from lakehouse_pipeline.config import get_spark_session, get_storage_paths
from lakehouse_pipeline.logger import get_logger

logger = get_logger("check_bronze")


def check_bronze() -> None:
    """Đọc và hiển thị thống kê bảng Delta tầng Bronze."""
    logger.info("=== KIỂM TRA DỮ LIỆU TẦNG BRONZE ===")

    spark = get_spark_session("BronzeCheck")
    paths = get_storage_paths()
    bronze_path = paths["bronze"]

    try:
        df = spark.read.format("delta").load(bronze_path)
        logger.info("Đọc thành công tầng Bronze tại: %s", bronze_path)
        logger.info("Tổng số dòng: %s", f"{df.count():,}")

        logger.info("Cấu trúc Schema:")
        df.printSchema()

        logger.info("5 dòng dữ liệu mẫu:")
        df.select("index", "order_id", "date", "status", "qty", "amount", "ingest_timestamp", "source_file").show(5, truncate=False)

    except Exception:
        logger.exception("Lỗi kiểm tra tầng Bronze")
    finally:
        spark.stop()


if __name__ == "__main__":
    check_bronze()
