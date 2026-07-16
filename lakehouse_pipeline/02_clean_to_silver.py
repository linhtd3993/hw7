"""
Pipeline làm sạch và chuẩn hóa dữ liệu cho tầng Silver.

Đọc dữ liệu thô từ tầng Bronze Delta Table, thực thi Schema Enforcement
(ép kiểu date, qty, amount, postal_code), khử trùng lặp theo khóa chính
(order_id, sku), và ghi dữ liệu sạch sang tầng Silver dưới dạng Delta Lake
phân vùng theo danh mục sản phẩm (category).
"""
from pyspark.sql.functions import col, to_date, coalesce, lit
from pyspark.sql.types import IntegerType, DoubleType, StringType, LongType
from pyspark.sql.utils import AnalysisException

from lakehouse_pipeline.config import (
    get_spark_session,
    get_storage_paths,
    DATE_FORMAT_SPARK,
    DEFAULT_CURRENCY,
    DEFAULT_STATUS,
    DEFAULT_COURIER_STATUS,
    DEDUP_KEYS,
    PARTITION_COL,
)
from lakehouse_pipeline.logger import get_logger

logger = get_logger("silver_cleaning")


def clean_to_silver() -> None:
    """Đọc Bronze, làm sạch, ép schema, khử trùng và ghi sang Silver.

    Quy trình xử lý:
        1. Đọc Bronze Delta Table.
        2. Ép kiểu: date → DateType, qty → IntegerType, amount → DoubleType.
        3. Xử lý Null: điền giá trị mặc định cho currency, courier_status, status.
        4. Chuẩn hóa ship_postal_code thành StringType sạch.
        5. Khử trùng lặp theo cặp khóa (order_id, sku).
        6. Ghi xuống Silver phân vùng theo category.

    Raises:
        AnalysisException: Nếu không đọc được Bronze Delta Table.
    """
    logger.info("=== BẮT ĐẦU PIPELINE LÀM SẠCH VÀ CHUẨN HÓA (TẦNG SILVER) ===")

    spark = get_spark_session("SilverCleaning")
    paths = get_storage_paths()

    bronze_path = paths["bronze"]
    silver_path = paths["silver"]

    logger.info("Bronze path : %s", bronze_path)
    logger.info("Silver path : %s", silver_path)

    try:
        # 1. Đọc dữ liệu từ tầng Bronze Delta Table
        df_bronze = spark.read.format("delta").load(bronze_path)
        row_count_bronze = df_bronze.count()
        logger.info("Số lượng dòng thô tại Bronze: %s", f"{row_count_bronze:,}")

        # 2. Schema Enforcement — ép kiểu và xử lý Null
        df_cleaned = (
            df_bronze
            .withColumn("date_clean", to_date(col("date"), DATE_FORMAT_SPARK))
            .withColumn("qty", col("qty").cast(IntegerType()))
            .withColumn("amount", coalesce(col("amount").cast(DoubleType()), lit(0.0)))
            .withColumn("ship_postal_code", col("ship_postal_code").cast(LongType()).cast(StringType()))
            .withColumn("currency", coalesce(col("currency"), lit(DEFAULT_CURRENCY)))
            .withColumn("courier_status", coalesce(col("courier_status"), lit(DEFAULT_COURIER_STATUS)))
            .withColumn("status", coalesce(col("status"), lit(DEFAULT_STATUS)))
            .drop("date", "unnamed__22")
            .withColumnRenamed("date_clean", "date")
        )

        # 3. Khử trùng lặp theo khóa chính tự nhiên
        logger.info("Khử trùng lặp theo khóa: %s", DEDUP_KEYS)
        df_deduplicated = df_cleaned.dropDuplicates(DEDUP_KEYS)

        # 4. Ghi xuống Silver — phân vùng theo category, mode overwrite (idempotent)
        logger.info("Ghi dữ liệu sạch lên Silver (phân vùng theo '%s')...", PARTITION_COL)
        (
            df_deduplicated.write
            .format("delta")
            .mode("overwrite")
            .partitionBy(PARTITION_COL)
            .save(silver_path)
        )

        # 5. Xác minh kết quả
        df_silver = spark.read.format("delta").load(silver_path)
        row_count_silver = df_silver.count()
        rows_removed = row_count_bronze - row_count_silver

        logger.info("=== HOÀN THÀNH PIPELINE TẦNG SILVER ===")
        logger.info("Tổng dòng sau làm sạch & khử trùng: %s (loại bỏ %d dòng)", f"{row_count_silver:,}", rows_removed)
        df_silver.printSchema()

    except AnalysisException:
        logger.exception("Lỗi phân tích Delta Table")
    except Exception:
        logger.exception("Lỗi không mong muốn ở tầng Silver")
    finally:
        spark.stop()


if __name__ == "__main__":
    clean_to_silver()
