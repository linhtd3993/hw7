import os
from pyspark.sql.functions import col, to_date, coalesce, lit
from pyspark.sql.types import IntegerType, DoubleType, StringType, LongType
from config import get_spark_session, get_storage_paths

def clean_to_silver():
    print("=== BẮT ĐẦU PIPELINE LÀM SẠCH VÀ CHUẨN HÓA (TẦNG SILVER) ===")
    
    spark = get_spark_session("SilverCleaning")
    paths = get_storage_paths()
    
    bronze_path = paths["bronze"]
    silver_path = paths["silver"]
    
    print(f"Đọc dữ liệu thô từ tầng Bronze: {bronze_path}")
    print(f"Đường dẫn ghi tầng Silver: {silver_path}")
    
    try:
        # 1. Đọc dữ liệu từ tầng Bronze Delta Table
        df_bronze = spark.read.format("delta").load(bronze_path)
        print(f"Số lượng dòng thô tại Bronze: {df_bronze.count():,}")
        
        # 2. Xử lý làm sạch và ép kiểu dữ liệu nghiêm ngặt (Schema Enforcement)
        df_cleaned = (
            df_bronze
            # Chuyển đổi định dạng ngày tháng 'MM-dd-yy' sang DateType
            .withColumn("date_clean", to_date(col("date"), "MM-dd-yy"))
            
            # Ép kiểu số lượng đơn hàng sang IntegerType
            .withColumn("qty", col("qty").cast(IntegerType()))
            
            # Ép kiểu số tiền sang DoubleType và điền giá trị 0.0 nếu Null (thường gặp khi trạng thái là Cancelled)
            .withColumn("amount", coalesce(col("amount").cast(DoubleType()), lit(0.0)))
            
            # Xử lý cột postal code: Tránh để dạng float/double (ví dụ: 12345.0), chuyển thành long rồi cast thành string sạch
            .withColumn("ship_postal_code", col("ship_postal_code").cast(LongType()).cast(StringType()))
            
            # Điền giá trị mặc định cho các cột chuỗi bị Null
            .withColumn("currency", coalesce(col("currency"), lit("INR")))
            .withColumn("courier_status", coalesce(col("courier_status"), lit("Unknown")))
            .withColumn("status", coalesce(col("status"), lit("Unknown")))
            
            # Loại bỏ các cột phụ hoặc cột lỗi (như unnamed__22 và cột date cũ)
            .drop("date", "unnamed__22")
            # Đổi tên cột date_clean thành date để giữ tên chuẩn
            .withColumnRenamed("date_clean", "date")
        )
        
        # 3. Khử trùng lặp (Deduplication) dựa trên khóa chính tự nhiên: order_id và sku
        # Trong e-commerce, một đơn hàng (order_id) có thể có nhiều mặt hàng khác nhau (sku) nhưng không thể trùng lặp sku trong cùng đơn hàng
        print("Đang tiến hành khử trùng lặp dữ liệu...")
        df_deduplicated = df_cleaned.dropDuplicates(["order_id", "sku"])
        
        # 4. Ghi dữ liệu xuống tầng Silver dưới định dạng Delta Lake
        # Thiết kế phân vùng (Partitioning) theo cột 'category' (Danh mục sản phẩm)
        # Sử dụng mode("overwrite") thay vì "append" để đảm bảo tính Idempotent (chạy lại script nhiều lần không bị nhân đôi dữ liệu)
        print(f"Đang ghi dữ liệu sạch lên tầng Silver (phân vùng theo 'category')...")
        (
            df_deduplicated.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("category")
            .save(silver_path)
        )
        
        # 5. Xác minh kết quả
        df_silver = spark.read.format("delta").load(silver_path)
        print("\n=== HOÀN THÀNH PIPELINE TẦNG SILVER ===")
        print(f"Tổng số dòng sau khi làm sạch & khử trùng: {df_silver.count():,}")
        
        # Hiển thị schema của tầng Silver
        print("\nCấu trúc Schema tầng Silver:")
        df_silver.printSchema()
        
    except Exception as e:
        print(f"Lỗi xảy ra ở tầng Silver: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    clean_to_silver()
