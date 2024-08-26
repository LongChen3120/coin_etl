## MÔ TẢ
- Dự án thu thập, xử lý, lưu trữ, trực quan thông tin dữ liệu tiền ảo trên sàn Binance.

## CÔNG NGHỆ SỬ DỤNG
- Python: websocket-client, kafka-python, pyspark
- Docker, Mysql, Kafka, Spark

## NỘI DUNG
### 1. Cài đặt môi trường
1.1. Cài đặt Docker, tạo các Container và cài đặt, chạy Mysql, Kafka, Spark.
    Mình sẽ hướng dẫn ở dự án khác

1.2. Tạo môi trường cho dự án
- Chạy lệnh sau để tạo môi trường ảo cho dự án:

```python -m venv etl_venv```
- Cài đặt các thư viện cho dự án:

```pip install -r requirements.txt```

### 2. Quy trình ETL
![coin_etl](https://github.com/user-attachments/assets/c7ce1492-a4c3-48a1-ac38-16a32543055b)
- **Producer**: Định nghĩa lớp kafkaProducer với 2 phương thức chính là khởi tạo producer và gửi message tới Kafka theo topic
- **source_websocket**: Khởi tạo các đối tượng từ lớp kafkaProducer, thực hiện gửi message nhận được từ Binance Kafka theo topic 
- **Consumer**: Định nghĩa lớp sparkConsumer với nhiều phương thức xử lý dữ liệu cho từng loại message, phương thức writeStream ghi dữ liệu theo lô vào Mysql
- **Database**: vì dữ liệu các message không có ràng buộc nên các bảng không có liên quan tới nhau, mỗi bảng sẽ lưu dữ liệu từ 1 topic. Dữ liệu tăng lên khá nhanh, mỗi bảng có thể tăng vài triệu hàng một ngày.
- **Superset**: Kết nối tới Mysql, tạo Dashboard, tạo Chart từ một số cột( ví dụ thời gian với giá của 1 loại tiền ảo), sau đó thêm Chart vào Dashboard.

### 3. Dashboard
- Dữ liệu được Superset hiển thị theo thời gian thực
![image](https://github.com/user-attachments/assets/855a0318-dd07-4a5b-a9cc-d0e8deac01f0)
