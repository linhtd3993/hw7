import os
import time
import shutil
import pandas as pd

# Định nghĩa đường dẫn
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "notebooks", "unlock-profits-with-e-commerce-sales-data", "Amazon Sale Report.csv")
LANDING_ZONE_DIR = os.path.join(BASE_DIR, "local_landing_zone")

def main():
    print("=== BẮT ĐẦU GIẢ LẬP LUỒNG DỮ LIỆU KAGGLE ===")
    print(f"Đọc dữ liệu thô từ: {DATA_PATH}")
    
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu gốc tại: {DATA_PATH}")
        
    # Tạo sạch thư mục landing zone cục bộ
    if os.path.exists(LANDING_ZONE_DIR):
        print(f"Xóa và làm sạch thư mục landing zone cũ tại: {LANDING_ZONE_DIR}")
        shutil.rmtree(LANDING_ZONE_DIR)
    os.makedirs(LANDING_ZONE_DIR, exist_ok=True)
    
    # Đọc dữ liệu gốc
    df = pd.read_csv(DATA_PATH, low_memory=False)
    print(f"Đọc thành công file dữ liệu với {len(df):,} dòng.")
    
    # Parse cột Date để sắp xếp theo trình tự thời gian
    df['parsed_date'] = pd.to_datetime(df['Date'], format='%m-%d-%y', errors='coerce')
    
    # Xử lý các dòng không parse được ngày (nếu có) bằng ngày mặc định
    df['parsed_date'] = df['parsed_date'].fillna(pd.Timestamp('2022-01-01'))
    
    # Sắp xếp theo trình tự thời gian tăng dần
    df = df.sort_values('parsed_date')
    
    # Lấy danh sách các ngày duy nhất để giả lập ghi
    unique_dates = df['Date'].unique()
    print(f"Tìm thấy {len(unique_dates)} ngày duy nhất để tiến hành giả lập luồng.")
    
    # Đếm số dòng mô phỏng thành công
    for i, date_str in enumerate(unique_dates):
        # Format tên file an toàn (thay thế ký tự đặc biệt nếu có)
        safe_date_str = str(date_str).replace('-', '_').replace(' ', '_').replace('/', '_')
        filename = f"amazon_sales_{safe_date_str}.csv"
        target_path = os.path.join(LANDING_ZONE_DIR, filename)
        
        # Lấy dữ liệu thuộc ngày này (loại bỏ cột parsed_date hỗ trợ sort)
        chunk_df = df[df['Date'] == date_str].drop(columns=['parsed_date'])
        
        print(f"[{i+1}/{len(unique_dates)}] Giả lập ngày {date_str}: Ghi {len(chunk_df):,} dòng vào {filename}...")
        chunk_df.to_csv(target_path, index=False)
        
        # Tạm dừng 2 giây giữa mỗi lần đẩy file để mô phỏng streaming
        time.sleep(2)
        
        # Để tránh chạy vô hạn khi chạy thử nghiệm, chúng ta có thể giới hạn ghi thử 5 ngày trước
        # Nếu muốn chạy toàn bộ, người dùng có thể cấu hình lại hoặc chạy trực tiếp.
        # Ở đây ta sẽ cho chạy hết nhưng có thể ngắt bằng Ctrl+C.
        
    print("=== HOÀN THÀNH GIẢ LẬP LUỒNG DỮ LIỆU KAGGLE ===")

if __name__ == "__main__":
    main()
