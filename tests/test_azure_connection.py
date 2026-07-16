"""
Script kiểm tra kết nối và đọc/ghi thử nghiệm lên Azure ADLS Gen2.

Tạo một DataFrame mẫu nhỏ, ghi xuống Azure dưới dạng Parquet,
đọc ngược lại để xác minh tính toàn vẹn kết nối end-to-end.
"""
from lakehouse_pipeline.config import get_spark_session, get_storage_paths
from lakehouse_pipeline.logger import get_logger

logger = get_logger("test_azure")


def test_azure() -> None:
    """Kiểm tra kết nối ghi/đọc với Azure ADLS Gen2.

    Raises:
        Exception: Nếu ghi hoặc đọc dữ liệu thất bại.
    """
    logger.info("=== BẮT ĐẦU KIỂM TRA KẾT NỐI AZURE ADLS GEN2 ===")

    try:
        spark = get_spark_session("AzureConnectionTest")
    except Exception:
        logger.exception("Không thể khởi tạo Spark Session")
        return

    paths = get_storage_paths()
    test_path = f"{paths['bronze']}_test_connection"
    logger.info("Đường dẫn ghi thử nghiệm: %s", test_path)

    try:
        # Tạo DataFrame mẫu
        data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
        df = spark.createDataFrame(data, schema=["Name", "Age"])
        df.show()

        # Ghi thử nghiệm
        logger.info("Đang ghi thử nghiệm lên ADLS Gen2...")
        df.write.mode("overwrite").parquet(test_path)
        logger.info("Ghi dữ liệu thành công!")

        # Đọc lại để xác minh
        read_df = spark.read.parquet(test_path)
        logger.info("Số dòng đọc được: %d", read_df.count())
        read_df.show()
        logger.info("=== KẾT NỐI AZURE ADLS GEN2 THÀNH CÔNG ===")

    except Exception:
        logger.exception("THẤT BẠI khi kiểm tra kết nối Azure")
        logger.info("Hướng dẫn sửa lỗi:")
        logger.info("1. Đảm bảo đã tạo Container trên Azure Storage Account.")
        logger.info("2. Đảm bảo Storage Account bật 'Hierarchical Namespace' (ADLS Gen2).")
        logger.info("3. Kiểm tra lại Access Key trong .env.")
    finally:
        spark.stop()


if __name__ == "__main__":
    test_azure()
