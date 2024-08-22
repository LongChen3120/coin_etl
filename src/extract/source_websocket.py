
import sys
import _env
import json
import websocket
sys.path.append(_env.SYS_PATH_APPEND)

from load import producer


def on_message(ws, message):
    data = json.loads(message)
    if type(data) == dict:
        if data["e"] == "aggTrade":
            # print(message)
            producer_aggtrade.send_message(message)
        elif data["e"] == "kline":
            producer_candlestick.send_message(message)
    elif type(data) == list:
        producer_price.send_message(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("CLOSE...")

def on_open(ws):
    print("Connection opened")
    ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": [
            "!ticker@arr"            # All Market Tickers Streams
            # "btcusdt@aggTrade",      # Aggregate Trade Streams cho BTCUSDT
            # "ethusdt@aggTrade",      # Aggregate Trade Streams cho ETHUSDT
            # "btcusdt@kline_1m",      # Kline/Candlestick Streams cho BTCUSDT với interval là 1 phút
            # "ethusdt@kline_5m"       # Kline/Candlestick Streams cho ETHUSDT với interval là 5 phút
        ],
        "id": 1
    }))

    ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": [
            # "!ticker@arr"            # All Market Tickers Streams
            "btcusdt@aggTrade",      # Aggregate Trade Streams cho BTCUSDT
            "ethusdt@aggTrade",      # Aggregate Trade Streams cho ETHUSDT
            "btcusdt@kline_1m",      # Kline/Candlestick Streams cho BTCUSDT với interval là 1 phút
            "ethusdt@kline_5m"       # Kline/Candlestick Streams cho ETHUSDT với interval là 5 phút
        ],
        "id": 2
    }))

if __name__ == "__main__":
    global producer_price, producer_aggtrade, producer_candlestick
    producer_price = producer.kafkaProducer(_env.TOPIC_CRYPTO_PRICE)
    producer_aggtrade = producer.kafkaProducer(_env.TOPIC_AGGTRADE)
    producer_candlestick = producer.kafkaProducer(_env.TOPIC_CANDLESTICK)


    websocket_url = "wss://fstream.binance.com/ws"
    ws = websocket.WebSocketApp(websocket_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    
    
    ws.run_forever()