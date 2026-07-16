"""
Module cấu hình trung tâm cho hệ thống Data Lakehouse.

Cung cấp:
- Khởi tạo SparkSession tích hợp Delta Lake và Azure ADLS Gen2.
- Đường dẫn lưu trữ (bronze / silver / gold) tự động chuyển đổi Local ↔ Azure.
- Hằng số pipeline và hàm tiện ích dùng chung.
"""
import os
import re
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame

from lakehouse_pipeline.logger import get_logger

logger = get_logger("config")

# ---------------------------------------------------------------------------
# Đồng bộ phiên bản Python cho PySpark worker và driver
# ---------------------------------------------------------------------------
_sys_executable = sys.executable
os.environ["PYSPARK_PYTHON"] = _sys_executable
os.environ["PYSPARK_DRIVER_PYTHON"] = _sys_executable

# ---------------------------------------------------------------------------
# Load biến môi trường từ file .env ở thư mục gốc dự án
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_dotenv_path = os.path.join(BASE_DIR, ".env")
load_dotenv(_dotenv_path)

# ---------------------------------------------------------------------------
# Biến cấu hình kết nối Azure
# ---------------------------------------------------------------------------
USE_AZURE: bool = os.getenv("USE_AZURE", "false").lower() == "true"
AZURE_ACCOUNT_NAME: str = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "")
AZURE_ACCOUNT_KEY: str = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")
AZURE_CONTAINER: str = os.getenv("AZURE_CONTAINER_NAME", "lakehouse")

# ---------------------------------------------------------------------------
# Đường dẫn Landing Zone cục bộ
# ---------------------------------------------------------------------------
LANDING_ZONE_PATH: str = os.path.join(BASE_DIR, "local_landing_zone")

# ---------------------------------------------------------------------------
# Hằng số Pipeline (tránh hardcode rải rác trong các file)
# ---------------------------------------------------------------------------
DATE_FORMAT_SPARK: str = "MM-dd-yy"        # Dùng trong PySpark to_date()
DATE_FORMAT_PANDAS: str = "%m-%d-%y"        # Dùng trong pandas to_datetime()
DEFAULT_CURRENCY: str = "INR"
DEFAULT_STATUS: str = "Unknown"
DEFAULT_COURIER_STATUS: str = "Unknown"
STREAMING_TIMEOUT_SECONDS: int = 30
SIMULATOR_DELAY_SECONDS: int = 2
DEDUP_KEYS: list[str] = ["order_id", "sku"]
PARTITION_COL: str = "category"


# ===================================================================
# Hàm tiện ích dùng chung
# ===================================================================

def clean_column_name(col_name: str) -> str:
    """Chuẩn hóa tên cột sang snake_case an toàn cho Delta Lake.

    Loại bỏ khoảng trắng thừa, thay thế mọi ký tự không phải chữ/số
    bằng dấu gạch dưới, và chuyển về chữ thường.

    Args:
        col_name: Tên cột gốc (ví dụ: ``"Order ID"``, ``"ship-city"``).

    Returns:
        Tên cột đã chuẩn hóa (ví dụ: ``"order_id"``, ``"ship_city"``).
    """
    return re.sub(r"[^a-z0-9]+", "_", col_name.strip().lower()).strip("_")


def clean_dataframe_columns(df: DataFrame) -> DataFrame:
    """Áp dụng chuẩn hóa tên cột cho toàn bộ DataFrame.

    Args:
        df: DataFrame đầu vào với tên cột chưa chuẩn hóa.

    Returns:
        DataFrame với tên cột đã được chuẩn hóa sang snake_case.
    """
    for col in df.columns:
        df = df.withColumnRenamed(col, clean_column_name(col))
    return df


# ===================================================================
# Khởi tạo SparkSession
# ===================================================================

def get_spark_session(app_name: str = "ECommerce_Lakehouse") -> SparkSession:
    """Khởi tạo và cấu hình SparkSession tích hợp Delta Lake.

    Tự động bổ sung thư viện Azure Hadoop Connector nếu ``USE_AZURE=true``
    trong file ``.env``.

    Args:
        app_name: Tên ứng dụng Spark hiển thị trên Spark UI.

    Returns:
        SparkSession đã được cấu hình sẵn.

    Raises:
        ValueError: Nếu ``USE_AZURE=true`` nhưng thiếu thông tin xác thực.
    """
    # Gói Maven cần thiết
    packages = ["io.delta:delta-spark_2.12:3.1.0"]

    if USE_AZURE:
        packages.append("org.apache.hadoop:hadoop-azure:3.3.4")

    spark_builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", ",".join(packages))
    )

    if USE_AZURE:
        if not AZURE_ACCOUNT_NAME or not AZURE_ACCOUNT_KEY:
            raise ValueError(
                "Phải điền AZURE_STORAGE_ACCOUNT_NAME và "
                "AZURE_STORAGE_ACCOUNT_KEY trong .env khi USE_AZURE=true"
            )

        dfs_endpoint = f"{AZURE_ACCOUNT_NAME}.dfs.core.windows.net"
        spark_builder = (
            spark_builder
            .config(f"fs.azure.account.auth.type.{dfs_endpoint}", "SharedKey")
            .config(f"fs.azure.account.key.{dfs_endpoint}", AZURE_ACCOUNT_KEY)
        )

    spark = spark_builder.getOrCreate()
    logger.info("SparkSession [%s] đã khởi tạo thành công (Spark %s)", app_name, spark.version)
    return spark


# ===================================================================
# Đường dẫn lưu trữ
# ===================================================================

def get_storage_paths() -> dict[str, str]:
    """Trả về đường dẫn thư mục lưu trữ cho bronze, silver, và gold.

    Tự động chuyển đổi giữa Local filesystem và Azure ADLS Gen2
    dựa trên biến ``USE_AZURE`` trong file ``.env``.

    Returns:
        Dict chứa các khóa ``"bronze"``, ``"silver"``, ``"gold"``,
        ``"landing_zone"`` và giá trị là đường dẫn tương ứng.
    """
    if USE_AZURE:
        base_path = f"abfss://{AZURE_CONTAINER}@{AZURE_ACCOUNT_NAME}.dfs.core.windows.net"
        paths = {
            "bronze": f"{base_path}/bronze",
            "silver": f"{base_path}/silver",
            "gold": f"{base_path}/gold",
            "landing_zone": LANDING_ZONE_PATH,
        }
    else:
        storage_dir = os.path.join(BASE_DIR, "lakehouse_storage")
        os.makedirs(storage_dir, exist_ok=True)
        paths = {
            "bronze": os.path.join(storage_dir, "bronze"),
            "silver": os.path.join(storage_dir, "silver"),
            "gold": os.path.join(storage_dir, "gold"),
            "landing_zone": LANDING_ZONE_PATH,
        }

    logger.info("Storage mode: %s", "Azure ADLS Gen2" if USE_AZURE else "Local")
    return paths


# ===================================================================
# Self-test khi chạy trực tiếp
# ===================================================================

if __name__ == "__main__":
    logger.info("Testing config.py Configuration")
    logger.info("USE_AZURE: %s", USE_AZURE)

    paths = get_storage_paths()
    for zone, path in paths.items():
        logger.info("  %s → %s", zone, path)

    logger.info("Khởi tạo thử nghiệm Spark session...")
    spark = get_spark_session("ConfigTest")
    logger.info("Spark version: %s", spark.version)
    spark.stop()
