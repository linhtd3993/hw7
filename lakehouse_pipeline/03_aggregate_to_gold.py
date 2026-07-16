"""
Pipeline tổng hợp dữ liệu phục vụ phân tích nghiệp vụ (tầng Gold).

Đọc dữ liệu sạch từ tầng Silver Delta Table và xây dựng 3 bảng tổng hợp:
    1. ``daily_revenue``      — Doanh thu, lượng hàng, số đơn theo ngày.
    2. ``category_revenue``   — Doanh thu theo danh mục sản phẩm.
    3. ``status_fulfillment`` — Phân phối trạng thái đơn hàng & kênh giao nhận.

Các bảng Gold được ghi dưới dạng Delta Table, tối ưu cho truy vấn SQL nhanh.
"""
import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as spark_sum, count, countDistinct, round as spark_round
from pyspark.sql.utils import AnalysisException

from lakehouse_pipeline.config import get_spark_session, get_storage_paths
from lakehouse_pipeline.logger import get_logger

logger = get_logger("gold_aggregation")


# ===================================================================
# Hàm xây dựng từng bảng Gold (tách nhỏ để dễ đọc và test)
# ===================================================================

def build_daily_revenue(df: DataFrame) -> DataFrame:
    """Tổng hợp doanh thu theo ngày.

    Args:
        df: DataFrame sạch từ tầng Silver.

    Returns:
        DataFrame tổng hợp theo cột ``date``.
    """
    return (
        df.groupby("date")
        .agg(
            spark_round(spark_sum("amount"), 2).alias("total_revenue"),
            spark_sum("qty").alias("total_qty"),
            countDistinct("order_id").alias("unique_orders"),
            count("order_id").alias("order_lines"),
        )
        .sort("date")
    )


def build_category_revenue(df: DataFrame) -> DataFrame:
    """Tổng hợp doanh thu theo danh mục sản phẩm.

    Args:
        df: DataFrame sạch từ tầng Silver.

    Returns:
        DataFrame tổng hợp theo cột ``category``, sắp xếp doanh thu giảm dần.
    """
    return (
        df.groupby("category")
        .agg(
            spark_round(spark_sum("amount"), 2).alias("total_revenue"),
            spark_sum("qty").alias("total_qty"),
            countDistinct("order_id").alias("unique_orders"),
            count("order_id").alias("order_lines"),
        )
        .sort("total_revenue", ascending=False)
    )


def build_status_fulfillment(df: DataFrame) -> DataFrame:
    """Thống kê phân phối trạng thái đơn hàng và hình thức vận chuyển.

    Args:
        df: DataFrame sạch từ tầng Silver.

    Returns:
        DataFrame tổng hợp theo cặp (``status``, ``fulfilment``).
    """
    return (
        df.groupby("status", "fulfilment")
        .agg(
            countDistinct("order_id").alias("unique_orders"),
            spark_round(spark_sum("amount"), 2).alias("total_revenue"),
        )
        .sort("status", "fulfilment")
    )


def _write_and_verify(df: DataFrame, path: str, label: str) -> None:
    """Ghi một bảng Gold xuống Delta và log kết quả xác minh.

    Args:
        df: DataFrame tổng hợp cần ghi.
        path: Đường dẫn lưu trữ Delta Table.
        label: Nhãn hiển thị trong log (ví dụ: ``"Daily Revenue"``).
    """
    logger.info("Ghi bảng [%s] lên: %s", label, path)
    df.write.format("delta").mode("overwrite").save(path)

    # Đọc lại để xác minh
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    verified = spark.read.format("delta").load(path)
    logger.info("Bảng [%s] — Số dòng: %d", label, verified.count())
    verified.show(5)


# ===================================================================
# Hàm chính
# ===================================================================

def aggregate_to_gold() -> None:
    """Đọc Silver, tổng hợp KPIs, và ghi xuống tầng Gold.

    Xây dựng 3 bảng tổng hợp nghiệp vụ dưới dạng Delta Table:
        - ``daily_revenue``: doanh thu theo ngày.
        - ``category_revenue``: doanh thu theo danh mục sản phẩm.
        - ``status_fulfillment``: phân phối trạng thái đơn hàng.

    Raises:
        AnalysisException: Nếu không đọc được Silver Delta Table.
    """
    logger.info("=== BẮT ĐẦU PIPELINE TỔNG HỢP (TẦNG GOLD) ===")

    spark = get_spark_session("GoldAggregation")
    paths = get_storage_paths()

    silver_path = paths["silver"]
    gold_path = paths["gold"]

    logger.info("Silver path : %s", silver_path)
    logger.info("Gold path   : %s", gold_path)

    try:
        # 1. Đọc bảng Delta sạch ở tầng Silver
        df_silver = spark.read.format("delta").load(silver_path)
        logger.info("Tổng số dòng đầu vào: %s", f"{df_silver.count():,}")

        # 2. Xây dựng các bảng Gold
        df_daily = build_daily_revenue(df_silver)
        df_category = build_category_revenue(df_silver)
        df_status = build_status_fulfillment(df_silver)

        # 3. Ghi các bảng Gold và xác minh
        _write_and_verify(df_daily, os.path.join(gold_path, "daily_revenue"), "Daily Revenue")
        _write_and_verify(df_category, os.path.join(gold_path, "category_revenue"), "Category Revenue")
        _write_and_verify(df_status, os.path.join(gold_path, "status_fulfillment"), "Status & Fulfillment")

        logger.info("=== PIPELINE TẦNG GOLD HOÀN THÀNH THÀNH CÔNG ===")

    except AnalysisException:
        logger.exception("Lỗi phân tích Delta Table")
    except Exception:
        logger.exception("Lỗi không mong muốn ở tầng Gold")
    finally:
        spark.stop()


if __name__ == "__main__":
    aggregate_to_gold()
