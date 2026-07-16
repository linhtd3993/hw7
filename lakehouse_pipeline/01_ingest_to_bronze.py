import os
from pyspark.sql.functions import current_timestamp, input_file_name
from config import get_spark_session, get_storage_paths

def ingest_to_bronze():
    print("=== BẮT ĐẦU PIPELINE NẠP DỮ LIỆU TẦNG BRONZE ===")
    
    spark = get_spark_session("BronzeIngestion")
    paths = get_storage_paths()
    
    # Kích hoạt tính năng tự động suy luận Schema cho Structured Streaming
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    
    landing_zone = paths["landing_zone"]
    bronze_path = paths["bronze"]
    checkpoint_path = os.path.join(bronze_path, "_checkpoint")
    
    print(f"Giám sát Landing Zone tại: {landing_zone}")
    print(f"Đường dẫn lưu trữ Bronze: {bronze_path}")
    print(f"Đường dẫn checkpoint: {checkpoint_path}")
    
    # 1. Đọc dữ liệu dạng Stream từ local_landing_zone (CSV)
    # Ở tầng Bronze, ta đọc thô hoàn toàn dưới dạng String (inferSchema=False) để bảo toàn dữ liệu gốc.
    df_stream = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(landing_zone)
    )
    
    # Chuẩn hóa tên cột để tránh lỗi Delta Lake (không cho phép khoảng trắng/ký tự đặc biệt)
    # Ví dụ: 'Order ID' -> 'order_id', 'Sales Channel ' -> 'sales_channel', 'ship-city' -> 'ship_city'
    for col in df_stream.columns:
        clean_col = col.strip().replace(" ", "_").replace("-", "_").replace(":", "_").replace("(", "_").replace(")", "_").lower()
        df_stream = df_stream.withColumnRenamed(col, clean_col)
    
    # 2. Thêm các cột metadata kỹ thuật để truy vết nguồn gốc (data lineage)
    df_bronze = (
        df_stream
        .withColumn("ingest_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )
    
    # 3. Ghi dữ liệu stream trực tiếp xuống Bronze định dạng Delta Lake kèm checkpoint
    print("Khởi chạy Structured Streaming Query...")
    query = (
        df_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(bronze_path)
    )
    
    print("Pipeline Structured Streaming đang chạy trong nền...")
    print("Đang nạp dữ liệu từ local_landing_zone lên Bronze (chờ khoảng 30 giây để hoàn tất dữ liệu hiện tại)...")
    
    try:
        # Cho stream chạy trong 30 giây để nạp hết 91 file CSV đang có
        query.awaitTermination(timeout=30)
        print("Đã hoàn tất nạp các lô dữ liệu hiện tại.")
    except Exception as e:
        print(f"Lỗi xảy ra trong quá trình streaming: {e}")
    finally:
        query.stop()
        spark.stop()
        print("=== PIPELINE BRONZE ĐÃ HOÀN THÀNH ===")

if __name__ == "__main__":
    ingest_to_bronze()
