"""
Bộ giả lập luồng dữ liệu Kaggle (Kaggle Stream Simulator).

Đọc file CSV gốc ``Amazon Sale Report.csv``, phân rã theo từng ngày
và ghi tuần tự các file CSV nhỏ vào thư mục ``local_landing_zone/``
để mô phỏng luồng dữ liệu phát sinh hàng ngày cho Spark Structured Streaming.
"""
import os
import shutil
import time

import pandas as pd

from lakehouse_pipeline.config import (
    BASE_DIR,
    LANDING_ZONE_PATH,
    DATE_FORMAT_PANDAS,
    SIMULATOR_DELAY_SECONDS,
)
from lakehouse_pipeline.logger import get_logger

logger = get_logger("simulator")

# Đường dẫn file dữ liệu gốc Kaggle
DATA_PATH: str = os.path.join(
    BASE_DIR, "notebooks", "unlock-profits-with-e-commerce-sales-data", "Amazon Sale Report.csv"
)


def _safe_filename(date_str: str) -> str:
    """Chuyển chuỗi ngày thành tên file an toàn.

    Args:
        date_str: Chuỗi ngày gốc (ví dụ: ``"04-01-22"``).

    Returns:
        Tên file an toàn (ví dụ: ``"amazon_sales_04_01_22.csv"``).
    """
    safe = str(date_str).replace("-", "_").replace(" ", "_").replace("/", "_")
    return f"amazon_sales_{safe}.csv"


def main() -> None:
    """Hàm chính: Đọc CSV gốc, chia nhỏ theo ngày, ghi tuần tự vào landing zone.

    Raises:
        FileNotFoundError: Nếu file dữ liệu gốc không tồn tại.
    """
    logger.info("=== BẮT ĐẦU GIẢ LẬP LUỒNG DỮ LIỆU KAGGLE ===")
    logger.info("Đọc dữ liệu thô từ: %s", DATA_PATH)

    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu gốc tại: {DATA_PATH}")

    # Tạo sạch thư mục landing zone cục bộ
    if os.path.exists(LANDING_ZONE_PATH):
        logger.info("Xóa và làm sạch landing zone cũ: %s", LANDING_ZONE_PATH)
        shutil.rmtree(LANDING_ZONE_PATH)
    os.makedirs(LANDING_ZONE_PATH, exist_ok=True)

    # Đọc dữ liệu gốc
    df = pd.read_csv(DATA_PATH, low_memory=False)
    logger.info("Đọc thành công: %s dòng.", f"{len(df):,}")

    # Parse và sắp xếp theo trình tự thời gian tăng dần
    df["parsed_date"] = pd.to_datetime(df["Date"], format=DATE_FORMAT_PANDAS, errors="coerce")
    df["parsed_date"] = df["parsed_date"].fillna(pd.Timestamp("2022-01-01"))
    df = df.sort_values("parsed_date")

    unique_dates = df["Date"].unique()
    total = len(unique_dates)
    logger.info("Tìm thấy %d ngày duy nhất để giả lập.", total)

    # Ghi tuần tự từng file CSV theo ngày
    for idx, date_str in enumerate(unique_dates, start=1):
        filename = _safe_filename(date_str)
        target_path = os.path.join(LANDING_ZONE_PATH, filename)

        chunk_df = df[df["Date"] == date_str].drop(columns=["parsed_date"])
        chunk_df.to_csv(target_path, index=False)

        logger.info("[%d/%d] Ngày %s: ghi %s dòng → %s", idx, total, date_str, f"{len(chunk_df):,}", filename)

        time.sleep(SIMULATOR_DELAY_SECONDS)

    logger.info("=== HOÀN THÀNH GIẢ LẬP LUỒNG DỮ LIỆU KAGGLE ===")


if __name__ == "__main__":
    main()
