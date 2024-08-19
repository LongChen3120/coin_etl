## MÔ TẢ
- Dự án thu thập, xử lý, lưu trữ, trực quan thông tin dữ liệu tiền ảo trên sàn Binance.

## Công nghệ sử dụng
- Python: websocket-client, kafka-python, pyspark
- Docker, Mysql, Kafka, Spark

## NỘI DUNG
### 1. Cài đặt môi trường
1.1. Cài đặt Docker, tạo các Container và cài đặt, chạy Mysql, Kafka, Spark
    Mình sẽ hướng dẫn ở dự án khác

1.2. Tạo môi trường cho dự án
- Chạy lệnh sau để tạo môi trường ảo cho dự án:

```python -m venv etl_venv```
- Cài đặt các thư viện cho dự án:

```pip install -r requirements.txt```

### 2. Quy trình ETL
![coin_etl](https://github.com/user-attachments/assets/aece0f5a-aca0-4818-8d37-adff050a358a)

### 3. Dashboard
![image](https://github.com/user-attachments/assets/855a0318-dd07-4a5b-a9cc-d0e8deac01f0)
