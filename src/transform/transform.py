import json


def filter_message_candle(message):
    try:
        if 'k' in message:
            filtered_message = {
                "Symbol": message["s"],
                "Kline_start_time": message["k"]["t"],
                "Open_price": message["k"]["o"],
                "Close_price": message["k"]["c"],
                "High_price": message["k"]["h"],
                "Low_price": message["k"]["l"],
                "Volume": message["k"]["q"]
            }
        
        return json.dumps(filtered_message)
    except:
        return false