
import sys
import json
import time
import datetime
import websocket
sys.path.append("../src")

import config

from utils import utils
from load import producer
from transform import transform


def on_message(ws, message):
    '''
    Hàm handler tốc độ gửi message vào kafka (vì binance gửi message cho mỗi symbol 0,25s một lần)
    -> hạn chế lại lượng message: mỗi phút, mỗi symbol được phép gửi vào kafka 1 message
    - sau khi tất cả các symbol đều được producer gửi message vào kafka, sleep cho tới phút tiếp theo 
    '''
    message_dict = json.loads(message)
    global numb_get_mess
    global time_old
    
    if datetime.datetime.now() > datetime.datetime.now().replace(second=55):
        # print(datetime.datetime.now())
        if utils.check_send_mes(numb_get_mess, message_dict):
            # print("message_raw: ", message_dict)
            filtered_message = transform.filter_message_candle(message_dict)
            print("message_filtered", filtered_message)
            producer_candle.send_message(filtered_message)
            numb_get_mess[message_dict["s"]]["numb"] += 1
            numb_get_mess[message_dict["s"]]["last_time"] = int(message_dict["k"]["t"])
            # print("time_now: ", datetime.datetime.now())
            print("numb_get_mess sau khi gui kafka: ", numb_get_mess)
        else:
            status_check_all_symbol = utils.handler_sleep(numb_get_mess, config.LIST_SYMBOL)
            # print(status_check_all_symbol)
            if status_check_all_symbol and ((datetime.datetime.now() - time_old).seconds > 60):
                for key in numb_get_mess:
                    numb_get_mess[key]["numb"] = 0
                time_old = datetime.datetime.now().replace(second=55, microsecond=0)
                print("time_now: ", datetime.datetime.now())
                print("numb_get_mess sau khi gui het vao kafka va sang phut moi: ", numb_get_mess)
            else:
                # print("passsssss")
                pass
    else:
        pass

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("CLOSE...")

def on_open(ws):
    print("Connection opened")
    ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": [
            "btcusdt@kline_1m",      # Kline/Candlestick Streams cho BTCUSDT với interval là 1 phút
            "ethusdt@kline_1m",
            "adausdt@kline_1m",
            "bnbusdt@kline_1m",
            "solusdt@kline_1m",
            "xrpusdt@kline_1m",
            "dogeusdt@kline_1m",
            "bchusdt@kline_1m"
        ],
        "id": 1
    }))

if __name__ == "__main__":
    numb_get_mess = config.NUMB_GET_MESS
    time_old = datetime.datetime.now().replace(second=55, microsecond=0)
    timestamp_ms = int(datetime.datetime.now().replace(second=00, microsecond=00).timestamp() * 1000)
    for key in numb_get_mess:
        numb_get_mess[key]["last_time"] = timestamp_ms

    producer_candle = producer.kafkaProducer(config.TOPIC_CANDLE_SPOT)


    websocket_url = "wss://stream.binance.com:443/ws"
    ws = websocket.WebSocketApp(websocket_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    
    
    ws.run_forever()