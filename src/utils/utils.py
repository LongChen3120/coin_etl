import logging
import datetime
import requests


def config_log(path_log, name_logger, lever_logger):
    '''
    cấu hình tạo logger riêng cho từng modul, mỗi modul ghi log ra file riêng
    '''
    logger = logging.getLogger(name_logger)
    logger.setLevel(lever_logger)

    # Tạo formatter để định dạng log
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Tạo file handler để ghi log vào file theo ngày
    file_handler = logging.FileHandler(path_log, encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Loại bỏ tất cả các handler ghi ra console
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            logger.removeHandler(handler)
    
    # Thêm file handler vào logger
    logger.addHandler(file_handler)
    return logger

def sleep_to_next_candle_1m():
    '''
    binance websocket gửi message mỗi 0.25s, gây dư thừa, không có nhu cầu cập nhật giá sớm đến thế
    -> hàm sleep cho đến giây thứ 30 của mỗi phút.
    '''
    current_time = datetime.datetime.now()

def handler_sleep(numb_get_mess, list_symbol):
    '''
    Hàm kiểm tra xem trong phút này, tất cả các symbol đã được producer gửi message vào kafka chưa
    - nếu gửi hết rồi thì return true và thời gian sleep cho tới phút tiếp theo
    - nếu chưa thì return false, không sleep
    '''
    for symbol in list_symbol:
        if numb_get_mess[symbol]["numb"] == 0:
            return False
    return True

def check_send_mes(numb_get_mess, message):
    '''
    Hàm kiểm tra xem message của symbol này có đủ điều kiện được producer gửi vào kafka không
    - nếu numb_get_mess >= 1 tức là symbol này trong phút này đã được gửi vào kafka -> return false 
    - nếu numb_get_mess == 0 tức là symbol này trong phút này chưa được gửi vào kafka -> return true 
    '''
    # print(numb_get_mess[message["s"]]["numb"])
    # print(message["k"]["t"], type(message["k"]["t"]))
    # print(numb_get_mess[message["s"]]["last_time"])
    if numb_get_mess[message["s"]]["numb"] == 0 and (int(message["k"]["t"]) - numb_get_mess[message["s"]]["last_time"] >= 60000):
        return True
    else: 
        return False

def send_message_tele(message):
    group_id="-4625713001" # chinh:  test: -4540368463
    token="6328411244:AAHxbHGvZ0W1n8vv-kwNFocSNoRUq6LuC9Y"
    try:
        apiURL = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={group_id}&text={message}"
        requests.get(apiURL).json()
    except Exception as e:
        print(e)

def calculate_time_sleep():
    '''
    Hàm tính toán thời gian từ hiện tại cho tới khi sang phút tiếp theo
    trả về số giây
    '''
    try:
        current_time = datetime.datetime.now()
        next_minute = current_time + datetime.timedelta(minutes=1)
        next_minute = next_minute.replace(second=5, microsecond=0)
        return int((next_minute - current_time).total_seconds())
    except Exception as e:
        print(e)
        return False
    