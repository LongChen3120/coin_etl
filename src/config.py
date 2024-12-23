from pyspark.sql.types import *
import logging

# VAL
BROKER_KAFKA_IP = "172.20.0.2:9092" # ip container chạy kafka
CONNECTION_STRING_MYSQL = "jdbc:mysql://192.168.110.164:3306/crypto"
USERNAME_MYSQL = "nguyen_long"
PASSWORD_MYSQL = "Finpros2023,"
TOPIC_CANDLE = "candle"
TOPIC_CANDLE_SPOT = "candle-spot"
TOPIC_CANDLE_FUTURES = "candle-futures"
SCHEMA_CANDLE = "crypto"
TABLE_NAME_CANDLE = "crypto"
TABLE_NAME_CANDLE_SPOT = "spot"

NAME_LOGGER_MODUL_CHECK_DATA = "check_data"

LEVEL_LOG_MODUL_CHECK_DATA = logging.INFO

PARAM_WEBSOCKET = {
    "method": "SUBSCRIBE",
        "params": [
            "btcusdt@kline_1m",      # Kline/Candlestick Streams cho BTCUSDT với interval là 1 phút
            "ethusdt@kline_1m",
            "adausdt@kline_1m",
            "bnbusdt@kline_1m",
            "solusdt@kline_1m",
            "xrpusdt@kline_1m",
            "dogeusdt@kline_1m",
            "avaxusdt@kline_1m",
            "trxusdt@kline_1m",
            "linkusdt@kline_1m",
            "uniusdt@kline_1m",
            "hbarusdt@kline_1m",
            "ltcusdt@kline_1m"
        ],
        "id": 1
}

LIST_SYMBOL = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BCHUSDT", "AVAXUSDT", "TRXUSDT", "LINKUSDT", "UNIUSDT", "HBARUSDT", "LTCUSDT", "ETCUSDT", "DOTUSDT"]

NUMB_GET_MESS = {
    "BTCUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "ETHUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "ADAUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "BNBUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "SOLUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "XRPUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "DOGEUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "BCHUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "AVAXUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "TRXUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "LINKUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "UNIUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "HBARUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "LTCUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "ETCUSDT": {
        "numb": 0,
        "last_time": 0
    },
    "DOTUSDT": {
        "numb": 0,
        "last_time": 0
    }
}

REPLACEMENTS_CANDLE = {
    "Symbol": "symbol",
    "Kline_start_time": "date",
    "Open_price": "OpenPrice",
    "Close_price": "ClosePrice",
    "High_price": "HighPrice",
    "Low_price": "LowPrice",
    "Volume": "TradeVolume"
}

SCHEMA_CANDLESTICK = StructType([
    StructField("Symbol", StringType(), True),
    StructField("Kline_start_time", LongType(), True),
    StructField("Open_price", StringType(), True),
    StructField("Close_price", StringType(), True),
    StructField("High_price", StringType(), True),
    StructField("Low_price", StringType(), True),
    StructField("Volume", StringType(), True)
])

# PATH
PATH_LOG_FILE_MODUL_CHECK_DATA = "../logs/check_data.log"

API_GET_LIST_SYMBOL = "http://192.168.110.164:8003/crypto/list_symbols"
API_GET_PRICE = "http://192.168.110.164:8003/crypto?symbol={}&days=0"