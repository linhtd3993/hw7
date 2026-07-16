import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Đồng bộ phiên bản Python cho PySpark worker và driver
sys_executable = sys.executable
os.environ["PYSPARK_PYTHON"] = sys_executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys_executable

# Load các biến môi trường từ file .env ở thư mục gốc
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path)

# Lấy các biến cấu hình kết nối
USE_AZURE = os.getenv("USE_AZURE", "false").lower() == "true"
AZURE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "lakehouse")

LANDING_ZONE_PATH = os.path.join(BASE_DIR, "local_landing_zone")

def get_spark_session(app_name="ECommerce_Lakehouse"):
    """
    Khởi tạo và cấu hình SparkSession tích hợp Delta Lake và Azure ADLS Gen2 (nếu bật).
    """
    # 1. Định nghĩa các gói Maven cần tải cho Spark
    # Phiên bản delta-spark 3.1.0 tương thích tốt với PySpark 3.5.0
    packages = [
        "io.delta:delta-spark_2.12:3.1.0"
    ]
    
    # 2. Nếu dùng Azure, bổ sung thư viện hadoop-azure để hỗ trợ giao thức abfss://
    if USE_AZURE:
        # Phiên bản hadoop-azure phù hợp với Hadoop đi kèm Spark 3.5.x
        packages.append("org.apache.hadoop:hadoop-azure:3.3.4")
        
    spark_builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
    )
    
    # Gán cấu hình packages cho Spark
    spark_builder = spark_builder.config("spark.jars.packages", ",".join(packages))
    
    if USE_AZURE:
        if not AZURE_ACCOUNT_NAME or not AZURE_ACCOUNT_KEY:
            raise ValueError("Phải điền AZURE_STORAGE_ACCOUNT_NAME và AZURE_STORAGE_ACCOUNT_KEY trong .env khi USE_AZURE=true")
        
        # Cấu hình bảo mật SharedKey để truy cập ADLS Gen2
        spark_builder = (
            spark_builder
            .config(f"fs.azure.account.auth.type.{AZURE_ACCOUNT_NAME}.dfs.core.windows.net", "SharedKey")
            .config(f"fs.azure.account.key.{AZURE_ACCOUNT_NAME}.dfs.core.windows.net", AZURE_ACCOUNT_KEY)
        )
        
    spark = spark_builder.getOrCreate()
    return spark

def get_storage_paths():
    """
    Trả về đường dẫn thư mục lưu trữ cho bronze, silver, và gold.
    Tự động chuyển đổi giữa Local và Azure dựa trên cấu hình USE_AZURE.
    """
    if USE_AZURE:
        base_path = f"abfss://{AZURE_CONTAINER}@{AZURE_ACCOUNT_NAME}.dfs.core.windows.net"
        return {
            "bronze": f"{base_path}/bronze",
            "silver": f"{base_path}/silver",
            "gold": f"{base_path}/gold",
            "landing_zone": LANDING_ZONE_PATH
        }
    else:
        # Đường dẫn cục bộ (Local) phục vụ phát triển/kiểm thử ngoại tuyến
        storage_dir = os.path.join(BASE_DIR, "lakehouse_storage")
        os.makedirs(storage_dir, exist_ok=True)
        return {
            "bronze": os.path.join(storage_dir, "bronze"),
            "silver": os.path.join(storage_dir, "silver"),
            "gold": os.path.join(storage_dir, "gold"),
            "landing_zone": LANDING_ZONE_PATH
        }

if __name__ == "__main__":
    # Test cấu hình
    print("Testing config.py Configuration:")
    print(f"USE_AZURE: {USE_AZURE}")
    paths = get_storage_paths()
    print("Storage Paths:")
    for zone, path in paths.items():
        print(f"  - {zone}: {path}")
        
    print("\nKhởi tạo thử nghiệm Spark session (quá trình này có thể tải maven dependencies ở lần đầu chạy)...")
    spark = get_spark_session("ConfigTest")
    print("Spark Session đã được khởi tạo thành công!")
    print(f"Spark version: {spark.version}")
    spark.stop()
