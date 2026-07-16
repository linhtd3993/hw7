# HW_7 — E-Commerce Data Lakehouse

Hệ thống Data Lakehouse thương mại điện tử xây dựng trên kiến trúc **Medallion 3 tầng** (Bronze → Silver → Gold) sử dụng **Apache Spark**, **Delta Lake**, và **Azure Data Lake Storage Gen2**.

## 📁 Cấu trúc dự án

```
HW_7/
├── data_simulator/                  # Giả lập luồng dữ liệu từ Kaggle
│   └── kaggle_stream_simulator.py
├── lakehouse_pipeline/              # Pipeline xử lý dữ liệu lõi
│   ├── config.py                    # Cấu hình Spark, Azure, hằng số
│   ├── logger.py                    # Module logging chuyên dụng
│   ├── 01_ingest_to_bronze.py       # Nạp dữ liệu thô (Structured Streaming)
│   ├── 02_clean_to_silver.py        # Làm sạch, ép schema, khử trùng
│   └── 03_aggregate_to_gold.py      # Tổng hợp KPIs cho Analytics
├── tests/                           # Kiểm thử kết nối và dữ liệu
│   ├── test_azure_connection.py
│   └── check_bronze.py
├── notebooks/                       # Jupyter Notebook (EDA, Time Travel)
│   ├── 00_download_and_eda.ipynb
│   └── query_and_versioning.ipynb
├── docs/                            # Tài liệu, diagram, log files
├── .env                             # Biến môi trường (Azure keys)
├── .gitignore
├── requirements.txt
└── README.md
```

## ⚙️ Cài đặt

```bash
# 1. Tạo môi trường ảo
conda create -y -n lakehouse_env python=3.10
conda activate lakehouse_env

# 2. Cài đặt thư viện
pip install -r requirements.txt
```

## 🔧 Cấu hình Azure

Tạo file `.env` tại thư mục gốc:

```env
AZURE_STORAGE_ACCOUNT_NAME="your_account_name"
AZURE_STORAGE_ACCOUNT_KEY="your_access_key"
USE_AZURE="true"
AZURE_CONTAINER_NAME="lakehouse"
```

> **Lưu ý:** Storage Account phải bật **Hierarchical Namespace** (ADLS Gen2) và tắt **Soft Delete**.

## 🚀 Chạy Pipeline

```bash
# Bước 1: Giả lập luồng dữ liệu
python -m data_simulator.kaggle_stream_simulator

# Bước 2: Nạp dữ liệu vào Bronze
python -m lakehouse_pipeline.01_ingest_to_bronze

# Bước 3: Làm sạch và ghi Silver
python -m lakehouse_pipeline.02_clean_to_silver

# Bước 4: Tổng hợp Gold
python -m lakehouse_pipeline.03_aggregate_to_gold
```

## 📊 Bộ dữ liệu

[Unlock Profits with E-Commerce Sales Data](https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data) — Amazon Sale Report (~128,975 dòng).
