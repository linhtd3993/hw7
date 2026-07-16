# 📘 TÀI LIỆU HỌC TẬP TOÀN DIỆN
# Hệ thống Data Lakehouse Thương mại Điện tử — HW_7

> Tài liệu này ghi lại **toàn bộ hành trình** xây dựng dự án từ con số 0:
> mục tiêu, kiến trúc, logic từng file, các lỗi thực tế đã gặp phải,
> cách khắc phục, và hướng dẫn chạy lại từ đầu khi clone về.

---

## MỤC LỤC

1. [Mục tiêu bài tập](#1-mục-tiêu-bài-tập)
2. [Kiến trúc tổng quan (Medallion Architecture)](#2-kiến-trúc-tổng-quan)
3. [Cấu trúc thư mục dự án](#3-cấu-trúc-thư-mục-dự-án)
4. [Giải thích logic từng file](#4-giải-thích-logic-từng-file)
5. [Quy trình thực hiện & Các vấn đề gặp phải](#5-quy-trình-thực-hiện--các-vấn-đề-gặp-phải)
6. [Hướng dẫn chạy code từ đầu (First-time Setup)](#6-hướng-dẫn-chạy-code-từ-đầu)
7. [Tổng kết kỹ thuật & Bài học rút ra](#7-tổng-kết-kỹ-thuật--bài-học-rút-ra)

---

## 1. MỤC TIÊU BÀI TẬP

Xây dựng một hệ thống **Data Lakehouse** độc lập trên nền tảng đám mây
**Azure Data Lake Storage Gen2**, sử dụng **Apache Spark** và **Delta Lake**
để xử lý bộ dữ liệu bán hàng thương mại điện tử (~128,975 dòng từ Kaggle).

### Yêu cầu kỹ thuật cần đạt được:
- **ACID Transactions**: Đảm bảo tính toàn vẹn dữ liệu khi ghi đọc đồng thời.
- **Schema Enforcement**: Ép kiểu dữ liệu nghiêm ngặt, từ chối dòng sai định dạng.
- **Data Versioning (Time Travel)**: Khả năng truy vấn lại trạng thái cũ của dữ liệu.
- **Structured Streaming**: Mô phỏng luồng dữ liệu "thời gian thực" thay vì batch tĩnh.
- **Kiến trúc Medallion 3 tầng**: Bronze → Silver → Gold.

### Bộ dữ liệu sử dụng:
**"Unlock Profits with E-Commerce Sales Data"** trên Kaggle
(https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data)

File chính: `Amazon Sale Report.csv` — chứa thông tin đơn hàng bán hàng trên Amazon
với các trường: Order ID, Date, Status, Category, Qty, Amount, Ship City/State, v.v.

---

## 2. KIẾN TRÚC TỔNG QUAN

```
                    ┌──────────────────────────────┐
                    │   Kaggle CSV Dataset (Raw)   │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │  kaggle_stream_simulator.py   │
                    │  Chia CSV gốc thành 91 file   │
                    │  nhỏ theo ngày, ghi tuần tự   │
                    │  vào local_landing_zone/       │
                    └──────────────┬───────────────┘
                                   │  (mỗi 2s ghi 1 file)
                    ┌──────────────▼───────────────┐
                    │  01_ingest_to_bronze.py       │
                    │  Spark Structured Streaming    │
                    │  đọc CSV mới → ghi Delta thô   │
                    │  + metadata (timestamp, file)  │
                    └──────────────┬───────────────┘
                                   │
              ┌────────────────────▼────────────────────┐
              │            TẦNG BRONZE (Azure)          │
              │  128,975 dòng — dữ liệu thô nguyên bản │
              │  Định dạng: Delta Lake                   │
              └────────────────────┬────────────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │  02_clean_to_silver.py        │
                    │  Schema Enforcement           │
                    │  + Deduplication               │
                    │  + Null handling               │
                    └──────────────┬───────────────┘
                                   │
              ┌────────────────────▼────────────────────┐
              │            TẦNG SILVER (Azure)          │
              │  128,968 dòng — sạch, chuẩn hóa         │
              │  Phân vùng theo: category                │
              │  Định dạng: Delta Lake                   │
              └────────────────────┬────────────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │  03_aggregate_to_gold.py      │
                    │  Tổng hợp KPIs nghiệp vụ     │
                    └──────────────┬───────────────┘
                                   │
              ┌────────────────────▼────────────────────┐
              │            TẦNG GOLD (Azure)            │
              │  3 bảng phân tích:                      │
              │  • daily_revenue (91 dòng)              │
              │  • category_revenue (9 dòng)            │
              │  • status_fulfillment (15 dòng)         │
              │  Định dạng: Delta Lake                   │
              └────────────────────────────────────────┘
```

### Tại sao lại dùng kiến trúc 3 tầng?
- **Bronze**: Lưu dữ liệu gốc nguyên bản, KHÔNG sửa đổi. Nếu pipeline tầng trên
  có bug, ta luôn có thể quay lại Bronze để xử lý lại (reprocess) mà không cần
  tải dữ liệu nguồn lần nữa.
- **Silver**: Dữ liệu "tin cậy" — đã lọc trùng, ép kiểu, xử lý null.
  Đây là tầng mà phần lớn analyst/engineer sẽ truy vấn hàng ngày.
- **Gold**: Dữ liệu tổng hợp sẵn, tối ưu cho dashboard/BI.
  Thay thế hoàn toàn Data Warehouse truyền thống.

---

## 3. CẤU TRÚC THƯ MỤC DỰ ÁN

```
HW_7/
├── data_simulator/                     # [PHÂN HỆ 1] Giả lập nguồn dữ liệu
│   ├── __init__.py
│   └── kaggle_stream_simulator.py      # Chia CSV gốc → 91 file theo ngày
│
├── lakehouse_pipeline/                 # [PHÂN HỆ 2] Pipeline xử lý lõi
│   ├── __init__.py
│   ├── logger.py                       # Module logging chuyên dụng
│   ├── config.py                       # Cấu hình Spark, Azure, hằng số
│   ├── 01_ingest_to_bronze.py          # Nạp dữ liệu thô (Streaming)
│   ├── 02_clean_to_silver.py           # Làm sạch, ép schema, khử trùng
│   └── 03_aggregate_to_gold.py         # Tổng hợp KPIs cho phân tích
│
├── tests/                              # Kiểm thử kết nối và dữ liệu
│   ├── test_azure_connection.py        # Test ghi/đọc Azure ADLS Gen2
│   └── check_bronze.py                # Xem thống kê bảng Bronze
│
├── notebooks/                          # Jupyter Notebook
│   ├── 00_download_and_eda.ipynb       # Tải dữ liệu + Phân tích khám phá
│   └── query_and_versioning.ipynb      # Kiểm thử Time Travel / ACID
│
├── docs/                               # Tài liệu, diagram, log pipeline
├── local_landing_zone/                 # [TỰ TẠO] Thư mục cổng chờ streaming
│
├── .env                                # Biến môi trường Azure (KHÔNG push Git)
├── .gitignore                          # Loại trừ file nhạy cảm & dữ liệu lớn
├── requirements.txt                    # Thư viện Python cần cài đặt
└── README.md                           # Hướng dẫn nhanh
```

---

## 4. GIẢI THÍCH LOGIC TỪNG FILE

---

### 4.1. `lakehouse_pipeline/logger.py` — Module Logging

**Mục đích**: Thay thế toàn bộ `print()` bằng hệ thống logging chuyên nghiệp.

**Logic hoạt động**:
- Tạo logger với tên module (ví dụ: `"bronze_ingestion"`, `"config"`).
- Ghi log đồng thời ra **console** (để người dùng xem trực tiếp) và **file**
  (lưu vào `docs/pipeline_YYYYMMDD.log` để truy vết sau này).
- Mỗi dòng log có format: `timestamp | tên_module | LEVEL | nội_dung`.

**Tại sao cần?**: Khi pipeline chạy trong production hoặc chạy đêm, không có ai
ngồi xem console. File log cho phép ta quay lại đọc chính xác chuyện gì đã xảy ra.

---

### 4.2. `lakehouse_pipeline/config.py` — Cấu hình Trung tâm

**Mục đích**: Tập trung toàn bộ cấu hình vào một nơi duy nhất.

**Logic hoạt động**:

```python
# 1. Đồng bộ phiên bản Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```
→ PySpark chạy 2 tiến trình: **Driver** (điều khiển) và **Worker** (xử lý).
Nếu hệ điều hành có nhiều phiên bản Python (ví dụ: Python 3.14 mặc định +
Python 3.10 trong conda), Spark sẽ lỗi "VERSION_MISMATCH". Dòng trên ép
cả hai tiến trình dùng cùng một Python.

```python
# 2. Đọc file .env
load_dotenv(dotenv_path)
USE_AZURE = os.getenv("USE_AZURE", "false").lower() == "true"
```
→ File `.env` chứa thông tin nhạy cảm (Access Key Azure). Thư viện `python-dotenv`
giúp đọc các biến này vào `os.environ` mà không cần export thủ công.

```python
# 3. Hằng số pipeline
DATE_FORMAT_SPARK = "MM-dd-yy"    # PySpark dùng format kiểu Java
DATE_FORMAT_PANDAS = "%m-%d-%y"   # pandas dùng format kiểu Python
DEDUP_KEYS = ["order_id", "sku"]  # Khóa chính để khử trùng
PARTITION_COL = "category"         # Cột phân vùng khi ghi Delta
```
→ Gom tất cả "magic values" vào một chỗ. Khi cần thay đổi (ví dụ đổi cột phân vùng
từ `category` sang `date`), chỉ sửa 1 dòng thay vì tìm khắp nơi.

```python
# 4. Hàm tiện ích: Chuẩn hóa tên cột
def clean_column_name(col_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", col_name.strip().lower()).strip("_")
```
→ Delta Lake KHÔNG cho phép tên cột chứa khoảng trắng, dấu gạch ngang, hoặc
ký tự đặc biệt. Hàm này biến `"Order ID"` → `"order_id"`, `"ship-city"` → `"ship_city"`.

```python
# 5. Khởi tạo SparkSession
packages = ["io.delta:delta-spark_2.12:3.1.0"]
if USE_AZURE:
    packages.append("org.apache.hadoop:hadoop-azure:3.3.4")
```
→ Spark tự động tải thư viện Java cần thiết từ Maven Central lần đầu chạy.
`delta-spark` cho tính năng Delta Lake, `hadoop-azure` cho giao thức `abfss://`.

```python
# 6. Đường dẫn linh hoạt
if USE_AZURE:
    base_path = f"abfss://{container}@{account}.dfs.core.windows.net"
else:
    base_path = os.path.join(BASE_DIR, "lakehouse_storage")
```
→ Cùng một code, chỉ đổi biến `.env` là chuyển từ chạy local sang cloud.
Rất tiện khi phát triển: test offline trước, rồi bật Azure lên khi sẵn sàng.

---

### 4.3. `data_simulator/kaggle_stream_simulator.py` — Bộ Giả lập

**Mục đích**: Biến 1 file CSV tĩnh thành luồng dữ liệu "thời gian thực".

**Tại sao cần?**: Bài tập yêu cầu xử lý dữ liệu streaming. Nhưng ta chỉ có
file CSV tĩnh từ Kaggle. Nếu chỉ dùng `spark.read.csv()` (batch), ta KHÔNG
chứng minh được khả năng "real-time" → thiếu điểm.

**Logic hoạt động**:
1. Đọc toàn bộ file `Amazon Sale Report.csv` bằng pandas.
2. Sắp xếp dữ liệu theo cột `Date` tăng dần (từ ngày cũ đến mới).
3. Lấy danh sách 91 ngày duy nhất.
4. Vòng lặp: Mỗi 2 giây ghi 1 file CSV tương ứng với 1 ngày vào
   thư mục `local_landing_zone/`.

```
local_landing_zone/
├── amazon_sales_03_31_22.csv   (171 dòng — ngày đầu tiên)
├── amazon_sales_04_01_22.csv   (1,470 dòng)
├── amazon_sales_04_02_22.csv   (1,555 dòng)
├── ...
└── amazon_sales_06_29_22.csv   (658 dòng — ngày cuối cùng)
```

Khi Spark Streaming (ở bước sau) giám sát thư mục này, mỗi file mới xuất hiện
sẽ được Spark tự động nhận diện và xử lý — giống như dữ liệu "đổ về" liên tục.

---

### 4.4. `01_ingest_to_bronze.py` — Pipeline Tầng Bronze

**Mục đích**: Nạp dữ liệu thô vào tầng Bronze — KHÔNG sửa đổi giá trị nghiệp vụ.

**Logic hoạt động**:
```python
# Bật tính năng tự suy luận schema cho streaming
spark.conf.set("spark.sql.streaming.schemaInference", "true")

# readStream thay vì read → giám sát liên tục thư mục
df_stream = spark.readStream.format("csv").option("header","true").load(landing_zone)

# Chuẩn hóa tên cột (bắt buộc cho Delta Lake)
df_stream = clean_dataframe_columns(df_stream)

# Thêm metadata kỹ thuật
df_bronze = df_stream \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .withColumn("source_file", input_file_name())

# writeStream → ghi liên tục với checkpoint
query = df_bronze.writeStream.format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .start(bronze_path)

query.awaitTermination(timeout=30)  # Chờ 30s để nạp hết
```

**Các điểm quan trọng**:
- `readStream` + `writeStream` = Spark Structured Streaming (khác với `read` + `write` là batch).
- `checkpointLocation` = Spark lưu tiến trình xử lý. Nếu crash giữa chừng rồi
  chạy lại, nó biết đã xử lý đến file nào → không bị trùng lặp (exactly-once).
- `ingest_timestamp` và `source_file` = metadata cho data lineage (truy vết nguồn gốc).

---

### 4.5. `02_clean_to_silver.py` — Pipeline Tầng Silver (TRỌNG TÂM)

**Mục đích**: Biến dữ liệu thô thành dữ liệu tin cậy.

**Logic hoạt động chi tiết**:

```python
# --- Schema Enforcement ---
# Ép cột Date từ chuỗi "04-01-22" sang kiểu ngày chuẩn
.withColumn("date_clean", to_date(col("date"), "MM-dd-yy"))

# Ép Qty sang Integer (loại bỏ giá trị không phải số)
.withColumn("qty", col("qty").cast(IntegerType()))

# Ép Amount sang Double, điền 0.0 nếu Null
# (Đơn hàng bị Cancelled thường có Amount = Null)
.withColumn("amount", coalesce(col("amount").cast(DoubleType()), lit(0.0)))

# Ship postal code: CSV đọc thành 12345.0 (float) → ép về "12345" (string)
.withColumn("ship_postal_code", col("ship_postal_code").cast(LongType()).cast(StringType()))
```

```python
# --- Xử lý Null ---
.withColumn("currency", coalesce(col("currency"), lit("INR")))        # Mặc định INR
.withColumn("courier_status", coalesce(col("courier_status"), lit("Unknown")))
.withColumn("status", coalesce(col("status"), lit("Unknown")))
```

```python
# --- Khử trùng ---
# Cặp (order_id, sku) phải duy nhất: 1 đơn hàng không thể có 2 dòng cùng sản phẩm
df_deduplicated = df_cleaned.dropDuplicates(["order_id", "sku"])
# Kết quả: 128,975 → 128,968 (loại bỏ 7 dòng trùng)
```

```python
# --- Ghi Delta với phân vùng ---
df_deduplicated.write.format("delta") \
    .mode("overwrite")         # Idempotent: chạy lại bao nhiêu lần cũng ra cùng kết quả
    .partitionBy("category")   # Tạo thư mục con theo category → truy vấn nhanh hơn
    .save(silver_path)
```

**Tại sao phân vùng theo `category`?**
Khi truy vấn `WHERE category = 'kurta'`, Spark CHỈ đọc thư mục `category=kurta/`
thay vì quét toàn bộ bảng. Với dữ liệu lớn, điều này giảm chi phí compute đáng kể.

---

### 4.6. `03_aggregate_to_gold.py` — Pipeline Tầng Gold

**Mục đích**: Tạo 3 bảng tổng hợp sẵn cho analytics/dashboard.

**Logic hoạt động**:
Mỗi bảng Gold là một phép `groupBy` + `agg` trên dữ liệu Silver:

| Bảng Gold | Nhóm theo | Chỉ số tính toán |
|-----------|-----------|------------------|
| `daily_revenue` | `date` | Tổng doanh thu, tổng qty, số đơn độc nhất |
| `category_revenue` | `category` | Tương tự, xếp hạng theo doanh thu giảm dần |
| `status_fulfillment` | `status`, `fulfilment` | Số đơn, doanh thu theo trạng thái |

```python
# Ví dụ: Bảng doanh thu theo ngày
df.groupby("date").agg(
    round(sum("amount"), 2).alias("total_revenue"),
    sum("qty").alias("total_qty"),
    countDistinct("order_id").alias("unique_orders"),
    count("order_id").alias("order_lines"),
)
```

**Lưu ý code refactor**: File gốc dùng `from pyspark.sql.functions import sum, round`
— điều này **che khuất** (shadow) hàm built-in `sum()` và `round()` của Python.
Sau refactor, ta đổi thành `spark_sum`, `spark_round` để tránh bug tiềm ẩn.

---

## 5. QUY TRÌNH THỰC HIỆN & CÁC VẤN ĐỀ GẶP PHẢI

Dưới đây là **6 vấn đề thực tế** đã gặp trong quá trình phát triển, sắp xếp
theo thứ tự thời gian:

---

### ❌ Vấn đề 1: Không tạo được Virtual Environment bằng `python -m venv`

**Triệu chứng**: Lệnh `python -m venv lakehouse_env` báo lỗi `ensurepip is not available`.

**Nguyên nhân**: Hệ điều hành dùng Python 3.14 hệ thống, nhưng chưa cài gói
`python3.14-venv`. Đồng thời, hệ thống cũng không có `pip` sẵn.

**Giải pháp**: Sử dụng **Conda** (đã được cài sẵn trên máy) thay vì `venv`:
```bash
conda create -y -n lakehouse_env python=3.10
conda activate lakehouse_env
pip install -r requirements.txt
```

**Bài học**: Luôn kiểm tra công cụ có sẵn trên máy trước khi bắt đầu.
Conda linh hoạt hơn venv vì nó tự quản lý cả phiên bản Python.

---

### ❌ Vấn đề 2: PySpark lỗi `PYTHON_VERSION_MISMATCH`

**Triệu chứng**: Spark khởi tạo thành công nhưng khi chạy `.show()` báo:
```
Python in worker has different version (3, 14) than that in driver 3.10
```

**Nguyên nhân**: Spark Driver chạy bằng Python 3.10 (trong conda env), nhưng
Spark Worker tự động chọn Python 3.14 (mặc định của hệ thống).

**Giải pháp**: Ép cả 2 tiến trình dùng cùng Python executable:
```python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```

**Bài học**: Đây là lỗi cực kỳ phổ biến khi chạy PySpark trên máy có nhiều
phiên bản Python. Luôn thiết lập biến môi trường này trong file config.

---

### ❌ Vấn đề 3: Azure trả lỗi `BlobStorageEvents or SoftDelete`

**Triệu chứng**: Ghi dữ liệu lên Azure báo lỗi HTTP 409:
```
"This endpoint does not support BlobStorageEvents or SoftDelete"
```

**Nguyên nhân**: Azure Storage Account có tính năng **Soft Delete** (tự giữ file đã xóa
để khôi phục) được bật mặc định. Giao thức `abfss://` (ADLS Gen2) yêu cầu
các thao tác rename nguyên tử (atomic rename) mà Soft Delete làm cản trở.

**Giải pháp**: Vào Azure Portal → Storage Account → Data protection
→ **Tắt** "Enable soft delete for blobs" → Save.

**Bài học**: ADLS Gen2 có yêu cầu nghiêm ngặt hơn Blob Storage thông thường.
Khi dùng Delta Lake trên Azure, LUÔN tắt Soft Delete và bật Hierarchical Namespace.

---

### ❌ Vấn đề 4: Azure trả lỗi `The specified filesystem does not exist`

**Triệu chứng**: Sau khi sửa lỗi Soft Delete, ghi tiếp thì báo HTTP 404:
```
"The specified filesystem does not exist"
```

**Nguyên nhân**: Trong ADLS Gen2, "filesystem" = "Container". File `.env` cấu hình
`AZURE_CONTAINER_NAME="lakehouse"` nhưng trên Azure chưa tạo Container tên `lakehouse`.

**Giải pháp**: Vào Azure Portal → Storage Account → Containers → + Container
→ Đặt tên `lakehouse` → Create.

**Bài học**: Spark không tự tạo Container trên Azure. Phải tạo thủ công trước.

---

### ❌ Vấn đề 5: Delta Lake từ chối tên cột có khoảng trắng

**Triệu chứng**: Streaming query báo lỗi:
```
DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES
Found invalid character(s) among ' ,;{}()\n\t=' in the column names
```

**Nguyên nhân**: File CSV gốc có tên cột như `"Order ID"`, `"Sales Channel "` (có cả
khoảng trắng thừa ở cuối!), `"ship-city"`. Delta Lake cấm các ký tự này.

**Giải pháp**: Thêm bước chuẩn hóa tên cột TRƯỚC khi ghi Delta:
```python
def clean_column_name(col_name):
    return re.sub(r"[^a-z0-9]+", "_", col_name.strip().lower()).strip("_")
```

**Bài học**: Khi làm việc với CSV "ngoài tự nhiên", LUÔN kiểm tra và chuẩn hóa
tên cột trước khi đưa vào hệ thống lưu trữ có schema nghiêm ngặt.

---

### ❌ Vấn đề 6: Spark Streaming yêu cầu Schema khi đọc CSV

**Triệu chứng**: Lệnh `spark.readStream.format("csv").load()` báo:
```
Schema must be specified when creating a streaming source DataFrame
```

**Nguyên nhân**: Khác với `spark.read` (batch), `spark.readStream` không tự suy luận
schema vì Spark không biết trước file nào sẽ đến. Ta phải cung cấp schema hoặc
bật cấu hình cho phép suy luận.

**Giải pháp**:
```python
spark.conf.set("spark.sql.streaming.schemaInference", "true")
```

**Bài học**: Streaming và Batch trong Spark có nhiều khác biệt nhỏ nhưng quan trọng.

---

## 6. HƯỚNG DẪN CHẠY CODE TỪ ĐẦU

### Yêu cầu hệ thống:
- **Python 3.10+** (khuyến nghị dùng Conda)
- **Java 11 hoặc 17** (PySpark cần JVM)
- Tài khoản **Azure** với Storage Account Gen2 (có Hierarchical Namespace)

### Bước 1: Clone dự án

```bash
git clone git@github.com:linhtd3993/hw7.git
cd hw7
```

### Bước 2: Tạo môi trường ảo và cài thư viện

```bash
# Tạo env bằng Conda (khuyến nghị)
conda create -y -n lakehouse_env python=3.10
conda activate lakehouse_env

# Cài thư viện
pip install -r requirements.txt
```

### Bước 3: Cấu hình Azure

Tạo file `.env` tại thư mục gốc (file này KHÔNG có trên Git vì lý do bảo mật):

```env
AZURE_STORAGE_ACCOUNT_NAME="ten_storage_account_cua_ban"
AZURE_STORAGE_ACCOUNT_KEY="access_key_cua_ban"
USE_AZURE="true"
AZURE_CONTAINER_NAME="lakehouse"
```

**Cách lấy thông tin trên Azure Portal:**
1. Đăng nhập https://portal.azure.com
2. Tìm Storage Account của bạn
3. **Account Name** = tên Storage Account (hiển thị trên trang Overview)
4. **Access Key**: Menu trái → Security + networking → Access keys → Show → Copy key1
5. **Container**: Menu trái → Data storage → Containers → + Container → Đặt tên `lakehouse`

**Quan trọng — Checklist cấu hình Azure:**
- ✅ Storage Account phải bật **Hierarchical Namespace** (trong tab Advanced khi tạo)
- ✅ **Tắt** Soft Delete (Data management → Data protection → Bỏ tích soft delete)
- ✅ Đã tạo Container tên `lakehouse` (hoặc tên khác, đổi trong .env cho khớp)

> **Nếu muốn chạy OFFLINE (không có Azure)**: Đổi `USE_AZURE="false"` trong `.env`.
> Dữ liệu sẽ được lưu vào thư mục `lakehouse_storage/` cục bộ trên máy.

### Bước 4: Tải dữ liệu Kaggle

Mở và chạy notebook `notebooks/00_download_and_eda.ipynb`, hoặc tải thủ công:
1. Truy cập https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data
2. Tải về và giải nén
3. Đặt thư mục vào `notebooks/unlock-profits-with-e-commerce-sales-data/`
4. Đảm bảo file `Amazon Sale Report.csv` nằm trong thư mục đó

### Bước 5: Chạy Pipeline theo thứ tự

```bash
# Bước 5.1: Giả lập luồng dữ liệu (chia CSV thành 91 file nhỏ theo ngày)
# ⏱ Mất khoảng 3 phút (91 file × 2s delay)
python -m data_simulator.kaggle_stream_simulator

# Bước 5.2: Nạp dữ liệu vào Bronze (Structured Streaming)
# ⏱ Mất khoảng 30 giây
python -m lakehouse_pipeline.01_ingest_to_bronze

# Bước 5.3: Làm sạch và ghi Silver (Schema Enforcement + Dedup)
# ⏱ Mất khoảng 20 giây
python -m lakehouse_pipeline.02_clean_to_silver

# Bước 5.4: Tổng hợp Gold (Aggregate KPIs)
# ⏱ Mất khoảng 20 giây
python -m lakehouse_pipeline.03_aggregate_to_gold
```

### Bước 6 (Tùy chọn): Kiểm tra kết nối Azure & Dữ liệu Bronze

```bash
python -m tests.test_azure_connection   # Kiểm tra ghi/đọc Azure
python -m tests.check_bronze            # Xem thống kê tầng Bronze
```

---

## 7. TỔNG KẾT KỸ THUẬT & BÀI HỌC RÚT RA

### Số liệu pipeline:

| Chỉ số | Giá trị |
|--------|---------|
| Dữ liệu gốc | 128,975 dòng / 91 ngày |
| Bronze (sau nạp) | 128,975 dòng |
| Silver (sau làm sạch) | 128,968 dòng (loại bỏ 7 trùng lặp) |
| Gold — Daily Revenue | 91 dòng (1 dòng/ngày) |
| Gold — Category Revenue | 9 dòng (9 danh mục) |
| Gold — Status Fulfillment | 15 dòng |

### Các công nghệ sử dụng:

| Công nghệ | Vai trò |
|-----------|---------|
| **PySpark 3.5.0** | Xử lý dữ liệu phân tán (distributed processing) |
| **Delta Lake 3.1.0** | Định dạng lưu trữ ACID trên Data Lake |
| **Azure ADLS Gen2** | Cloud storage với Hierarchical Namespace |
| **Structured Streaming** | Xử lý luồng dữ liệu gần thời gian thực |
| **pandas** | Đọc/chia file CSV cho simulator |
| **python-dotenv** | Quản lý biến môi trường an toàn |

### Các nguyên tắc thiết kế đã áp dụng:

1. **Idempotent**: Tầng Silver và Gold dùng `mode("overwrite")` → chạy lại bao nhiêu
   lần cũng ra kết quả giống nhau, không bị nhân đôi dữ liệu.
2. **Exactly-once**: Tầng Bronze dùng checkpoint → dù pipeline crash giữa chừng,
   khi chạy lại sẽ tiếp tục từ file chưa xử lý.
3. **Schema-on-Write**: Ép kiểu dữ liệu TRƯỚC khi ghi vào Silver, không chấp nhận
   dữ liệu "bẩn" đi qua.
4. **Separation of Concerns**: Mỗi tầng có trách nhiệm riêng biệt, dễ debug.
5. **Configuration as Code**: Mọi cấu hình nằm trong `.env` và `config.py`,
   không hardcode rải rác trong logic xử lý.

### Clean Code Refactor đã thực hiện:

| Trước | Sau |
|-------|-----|
| `print("Lỗi: " + str(e))` | `logger.exception("Lỗi phân tích Delta")` |
| `from config import ...` (dễ gãy) | `from lakehouse_pipeline.config import ...` |
| Inline `col.replace(" ","_")...` | `clean_column_name()` + `clean_dataframe_columns()` |
| `time.sleep(2)` hardcode | `SIMULATOR_DELAY_SECONDS = 2` trong config |
| `except Exception as e: print(e)` | `except AnalysisException: logger.exception(...)` |
| Không có docstring | Docstring Google-style + Type Hints trên mọi hàm |
| File test lẫn với pipeline | Tách ra thư mục `tests/` riêng biệt |
