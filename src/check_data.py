"""
- chương trình kiểm tra dữ liệu crypto và đưa ra cảnh báo
- nếu dữ liệu có gap lớn hơn 2p thì đưa ra cảnh báo telegram
"""
import sys
import pandas
import datetime
import time
import requests
sys.path.append("../src")

import config
from load import load_file
from utils import utils


def init_logger():
    global logger
    logger = utils.config_log(config.PATH_LOG_FILE_MODUL_CHECK_DATA, config.NAME_LOGGER_MODUL_CHECK_DATA, config.LEVEL_LOG_MODUL_CHECK_DATA)

def get_list_symbol():
    try:
        list_symbol = []
        response = requests.get(config.API_GET_LIST_SYMBOL)
        data = response.json()["data"]
        list_symbol = [item["symbol"] for item in data]
        if list_symbol:
            return list_symbol
        else:
            logger.warning(f"List symbol rỗng: {list_symbol}")
            return False
    except Exception as e:
        logger.warning(f"Lỗi khi chạy hàm get_list_symbol: \n{e}")
        return False

def loop_get_price(symbol):
    '''
    Hàm loop lấy giá của symbol tối đa 5 lần nếu không lấy được
    '''
    try:
        for i in range(5):
            try:
                response = requests.get(config.API_GET_PRICE.format(symbol))
                response = response.json()
                print(response)
                if response["code"] == 200:
                    data = pandas.DataFrame(response["data"])
                    if not data.empty:
                        return True, data
                    else:
                        logger.warning(f"Loop lần thứ {i + 1} lỗi: Dataframe rỗng")
                        time.sleep(1)
                        continue
                else:
                    logger.warning(f"Loop lần thứ {i + 1} lỗi: status code = {response['code']}")
                    time.sleep(1)
                    continue
            except:
                pass
        logger.warning(f"Lỗi sau 5 lần loop")
        return False, pandas.DataFrame()
    except Exception as e:
        logger.warning(f"Lỗi khi chạy hàm loop_get_price: \n{e}")
        return False, pandas.DataFrame()

def handler_check_price(symbol):
    status, data = loop_get_price(symbol)
    if not status:
        message = f"Lỗi không lấy được dữ liệu giá của symbol {symbol}"
        logger.warning(message)
        return False, message
    try:
        last_five_row = data.tail(5).copy()
        if not last_five_row.empty:
            # chuyển kiểu thời gian, sắp xếp thời gian giảm dần
            last_five_row["Date"] = pandas.to_datetime(last_five_row["Date"], format='%Y-%m-%dT%H:%M:%S')
            last_five_row = last_five_row.sort_values(by="Date", ascending=False)

            # Lấy thời gian của dòng đầu tiên (mới nhất)
            newest_time = last_five_row.iloc[0]["Date"]
            current_time = datetime.datetime.now()
            time_gap = (current_time - newest_time).total_seconds()
            # tính khoảng cách thời gian giữa các dòng
            time_different = last_five_row["Date"].diff().dt.total_seconds()
            # Nếu thời gian trong hàng mới nhất cách biệt quá 5 phút so với hiện tại
            if time_gap > 180:
                message = f"Không có cập nhật mới trong hơn 3 phút symbol {symbol}, thời gian gần nhất: {newest_time}"
                logger.warning(message)
                return False, message 
            # Nếu thời gian giữa các dòng có gap > 5p
            elif (time_different > 120).any():
                message = f"Gap thời gian symbol {symbol}"
                logger.warning(message)
                return False, message
            # kiểm tra có hàng nào null không
            elif last_five_row.isnull().any().any():
                message = f"Dữ liệu bị Null symbol {symbol}"
                logger.warning(message)
                return False, message
            else:
                return (True, "")
        else:
            message = f"Dữ liệu bị Null symbol {symbol}"
            logger.warning(message)
            return False, message
    except Exception as e:
        message = f"""Lỗi khi chạy hàm handler_check_price cho symbol {symbol} \n
                       Ngoại lệ: {e}"""
        logger.warning(message)
        return False, message

if __name__ == "__main__":
    # utils.send_message_tele("hello")
    init_logger()
    logger.warning("__________________________________ RUN MAIN __________________________________")
    message_queue = ""
    list_symbol = get_list_symbol()
    if list_symbol:
        while True:
            for symbol in list_symbol:
                print("check symbol", symbol)
                status, message = handler_check_price(symbol)
                if not status:
                    utils.send_message_tele(message)
            time_sleep = utils.calculate_time_sleep()
            print("time_sleep", time_sleep)
            if time_sleep:
                time.sleep(time_sleep)
            else:
                message_temp = f"Lỗi khi tính toán thời gian sleep cho tới phút tiếp theo, time_sleep: {time_sleep}"
                message_queue += f"{message_temp}\n"
                logger.warning(message_temp)
                break
        message_temp = "Dừng chương trình"
        message_queue += f"{message_temp}\n"
        logger.warning(message_temp)
    else:
        message_temp = "List symbol rỗng dừng chương trình"
        message_queue += f"{message_temp}\n"
        logger.warning(message_temp)

    utils.send_message_tele(message_queue)
