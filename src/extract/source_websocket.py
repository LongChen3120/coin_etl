
import sys
import json
import websocket
sys.path.append("D:/Python/2024/coin_etl/src")

import config

from load import producer


def on_message(ws, message):
    # data = json.loads(message)
    producer_1.send_message(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("CLOSE...")

def on_open(ws):
    print("Connection opened")
    ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": [
            "!ticker@arr"
        ],
        "id": 1
    }))

if __name__ == "__main__":
    global producer_1
    producer_1 = producer.kafkaProducer(config.TOPIC_CRYPTO_PRICE)

    websocket_url = "wss://fstream.binance.com/ws"
    ws = websocket.WebSocketApp(websocket_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    
    
    ws.run_forever()