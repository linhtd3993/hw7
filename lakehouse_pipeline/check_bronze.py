import sys
import os

# Bổ sung thư mục hiện tại vào sys.path để import được config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lakehouse_pipeline.config import get_spark_session, get_storage_paths

def check_bronze():
    print("=== KIỂM TRA DỮ LIỆU TẦNG BRONZE TRÊN AZURE ADLS GEN2 ===")
    
    spark = get_spark_session("BronzeCheck")
    paths = get_storage_paths()
    bronze_path = paths["bronze"]
    
    try:
        # Đọc dữ liệu bảng Delta từ tầng Bronze
        df = spark.read.format("delta").load(bronze_path)
        print(f"Đọc thành công bảng Delta tầng Bronze tại: {bronze_path}")
        print(f"Tổng số dòng dữ liệu hiện tại ở tầng Bronze: {df.count():,}")
        
        # Xem cấu trúc Schema của Bronze
        print("\nCấu trúc Schema của Bronze (đã chuẩn hóa tên cột):")
        df.printSchema()
        
        # Xem 5 dòng dữ liệu mẫu
        print("\nHiển thị 5 dòng dữ liệu mẫu:")
        df.select("index", "order_id", "date", "status", "qty", "amount", "ingest_timestamp", "source_file").show(5, truncate=False)
        
    except Exception as e:
        print(f"Lỗi kiểm tra tầng Bronze: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    check_bronze()
