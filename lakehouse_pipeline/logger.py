"""
Module logging chuyên dụng cho hệ thống Data Lakehouse.

Cung cấp logger thống nhất với khả năng ghi ra cả console và file,
bao gồm timestamp, tên module, và mức độ log rõ ràng.
"""
import logging
import os
from datetime import datetime


def get_logger(name: str) -> logging.Logger:
    """Khởi tạo và trả về một logger được cấu hình sẵn.

    Logger ghi log đồng thời ra console (stdout) và file log hàng ngày
    tại thư mục ``docs/`` của dự án.

    Args:
        name: Tên định danh của logger (thường là tên module hoặc pipeline).

    Returns:
        logging.Logger: Đối tượng logger đã được cấu hình.
    """
    logger = logging.getLogger(name)

    # Tránh thêm handler trùng lặp nếu logger đã được khởi tạo trước đó
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler — ghi log ra thư mục docs/
    log_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs"
    )
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f"pipeline_{datetime.now():%Y%m%d}.log"),
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
