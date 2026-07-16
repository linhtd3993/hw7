import sys
import os

# Bổ sung thư mục hiện tại vào sys.path để import được config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lakehouse_pipeline.config import get_spark_session, get_storage_paths

def test_azure():
    print("=== BẮT ĐẦU KIỂM TRA KẾT NỐI AZURE ADLS GEN2 ===")
    
    # 1. Khởi tạo Spark Session tích hợp Azure và Delta
    try:
        print("Đang khởi tạo Spark Session...")
        spark = get_spark_session("AzureConnectionTest")
        print("Khởi tạo Spark thành công!")
    except Exception as e:
        print(f"LỖI KHỞI TẠO SPARK: {e}")
        return
        
    # 2. Lấy đường dẫn lưu trữ
    paths = get_storage_paths()
    test_path = f"{paths['bronze']}_test_connection"
    print(f"Đường dẫn ghi thử nghiệm: {test_path}")
    
    # 3. Tạo DataFrame mẫu
    try:
        print("Đang tạo DataFrame mẫu...")
        data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
        columns = ["Name", "Age"]
        df = spark.createDataFrame(data, schema=columns)
        df.show()
    except Exception as e:
        print(f"LỖI TẠO DATAFRAME: {e}")
        spark.stop()
        return

    # 4. Ghi thử nghiệm lên Azure ADLS Gen2
    try:
        print(f"Đang ghi thử nghiệm lên ADLS Gen2 tại {test_path}...")
        # Ghi dạng parquet thô để kiểm tra kết nối cơ bản trước
        df.write.mode("overwrite").parquet(test_path)
        print("Ghi dữ liệu thành công!")
    except Exception as e:
        print("\n[!] THẤT BẠI KHI GHI LÊN AZURE ADLS GEN2!")
        print(f"Lỗi chi tiết: {e}")
        print("\nHướng dẫn sửa lỗi:")
        print("1. Hãy chắc chắn rằng bạn đã tạo Container tên là 'lakehouse' trên Azure Storage Account 'linhhw' của mình.")
        print("2. Đảm bảo Storage Account của bạn bật 'Hierarchical Namespace' (ADLS Gen2).")
        print("3. Kiểm tra lại Access Key xem đã chính xác chưa.")
        spark.stop()
        return

    # 5. Đọc thử nghiệm lại
    try:
        print("Đang đọc lại dữ liệu vừa ghi từ Azure...")
        read_df = spark.read.parquet(test_path)
        print(f"Số dòng đọc được từ Azure: {read_df.count()}")
        read_df.show()
        print("\n=== KẾT NỐI VÀ GHI ĐỌC LÊN AZURE ADLS GEN2 THÀNH CÔNG!!! ===")
    except Exception as e:
        print(f"Lỗi đọc lại dữ liệu: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    test_azure()
