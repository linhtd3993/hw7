import os
from pyspark.sql.functions import sum, count, countDistinct, round
from config import get_spark_session, get_storage_paths

def aggregate_to_gold():
    print("=== BẮT ĐẦU PIPELINE TỔNG HỢP (TẦNG GOLD) ===")
    
    spark = get_spark_session("GoldAggregation")
    paths = get_storage_paths()
    
    silver_path = paths["silver"]
    gold_path = paths["gold"]
    
    print(f"Đọc dữ liệu sạch từ tầng Silver: {silver_path}")
    print(f"Đường dẫn ghi tầng Gold: {gold_path}")
    
    try:
        # 1. Đọc bảng Delta sạch ở tầng Silver
        df_silver = spark.read.format("delta").load(silver_path)
        print(f"Tổng số dòng dữ liệu sạch làm đầu vào: {df_silver.count():,}")
        
        # 2. Xây dựng Bảng Gold 1: Doanh thu theo Ngày (Daily Revenue)
        # Tính tổng doanh thu, tổng số lượng hàng, số đơn hàng duy nhất và số lượng dòng đơn
        print("\n[Bảng Gold 1] Đang tổng hợp doanh thu theo ngày...")
        df_daily_revenue = (
            df_silver.groupby("date")
            .agg(
                round(sum("amount"), 2).alias("total_revenue"),
                sum("qty").alias("total_qty"),
                countDistinct("order_id").alias("unique_orders"),
                count("order_id").alias("order_lines")
            )
            .sort("date")
        )
        
        # 3. Xây dựng Bảng Gold 2: Doanh thu theo Danh mục sản phẩm (Category Revenue)
        print("[Bảng Gold 2] Đang tổng hợp doanh thu theo danh mục sản phẩm...")
        df_category_revenue = (
            df_silver.groupby("category")
            .agg(
                round(sum("amount"), 2).alias("total_revenue"),
                sum("qty").alias("total_qty"),
                countDistinct("order_id").alias("unique_orders"),
                count("order_id").alias("order_lines")
            )
            .sort("total_revenue", ascending=False)
        )
        
        # 4. Xây dựng Bảng Gold 3: Thống kê trạng thái đơn hàng & Hình thức vận chuyển (Status & Fulfillment)
        print("[Bảng Gold 3] Đang tổng hợp phân phối trạng thái đơn hàng và hình thức vận chuyển...")
        df_status_fulfillment = (
            df_silver.groupby("status", "fulfilment")
            .agg(
                countDistinct("order_id").alias("unique_orders"),
                round(sum("amount"), 2).alias("total_revenue")
            )
            .sort("status", "fulfilment")
        )
        
        # 5. Ghi các bảng tổng hợp lên tầng Gold dưới dạng Delta Tables
        daily_gold_path = os.path.join(gold_path, "daily_revenue")
        category_gold_path = os.path.join(gold_path, "category_revenue")
        status_gold_path = os.path.join(gold_path, "status_fulfillment")
        
        print(f"\nGhi Bảng Gold 1 lên: {daily_gold_path}")
        df_daily_revenue.write.format("delta").mode("overwrite").save(daily_gold_path)
        
        print(f"Ghi Bảng Gold 2 lên: {category_gold_path}")
        df_category_revenue.write.format("delta").mode("overwrite").save(category_gold_path)
        
        print(f"Ghi Bảng Gold 3 lên: {status_gold_path}")
        df_status_fulfillment.write.format("delta").mode("overwrite").save(status_gold_path)
        
        # 6. Đọc kiểm tra lại dữ liệu để xác minh
        print("\n=== XÁC MINH KẾT QUẢ TẦNG GOLD ===")
        
        df1 = spark.read.format("delta").load(daily_gold_path)
        print(f"Bảng Gold 1 (Daily Revenue) - Số dòng: {df1.count()}")
        df1.show(5)
        
        df2 = spark.read.format("delta").load(category_gold_path)
        print(f"Bảng Gold 2 (Category Revenue) - Số dòng: {df2.count()}")
        df2.show(5)
        
        df3 = spark.read.format("delta").load(status_gold_path)
        print(f"Bảng Gold 3 (Status & Fulfillment) - Số dòng: {df3.count()}")
        df3.show(5)
        
        print("\n=== PIPELINE TẦNG GOLD HOÀN THÀNH THÀNH CÔNG ===")
        
    except Exception as e:
        print(f"Lỗi xảy ra ở tầng Gold: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    aggregate_to_gold()
