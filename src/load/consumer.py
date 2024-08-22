import sys
import _env
sys.path.append(_env.SYS_PATH_APPEND)
import kafka

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import thread_pool




class kafkaConsumer():
    broker_ip = _env.BROKER_KAFKA_IP

    def __init__(self, topic, id_group) -> None:
        self.topic = topic
        self.consumer = kafka.KafkaConsumer(
            self.topic,
            bootstrap_servers = self.broker_ip,
            auto_offset_reset = "latest",
            group_id = id_group,
            enable_auto_commit = True
        )
    
    def print_message(self):
        try:
            for message in self.consumer:
                print("value: ", message.value.decode('utf-8'),
                    "\ntopic: ", message.topic,
                    "\npartition: ", message.partition,
                    "\nkey: ", message.key,
                    "\noffset: ", message.offset,
                    "\n\n")
        except KeyboardInterrupt:
            print("consumer closed")
            self.consumer.close()

class sparkConsumer():
    connection_string = _env.CONNECTION_STRING_MYSQL
    def __init__(self, ss) -> None:
        self.ss = ss
        self.df = None

    def read_stream(self, topic_name):
        self.df = self.ss.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", _env.BROKER_KAFKA_IP) \
                .option("subscribe", topic_name) \
                .option("startingOffsets", "latest") \
                .load()
    
    def replace_key(self, replacements):
        for old_char, new_char in replacements.items():
            # thay key trong message trong df["value"]
            self.df = self.df.withColumn("value", regexp_replace(self.df["value"], old_char, new_char))
        
    def replace_1(self):
        pass

    def replace_2(self):
        pass

    def structed_message(self, schema):
        '''
        dung from_json phan tich cu phap message, chuyen thanh cau truc du lieu duoc xac dinh trong schema
        '''
        self.df = self.df.withColumn("structed_message", from_json(self.df["value"], schema)).select("structed_message")

    def explode_message(self):
        '''
        chuyen du lieu dang mang [{}, {},..] sang moi hang mot doi tuong {}
        '''
        self.df = self.df.withColumn("explode_message", explode(self.df["structed_message"])).select("explode_message")

    def format_message_tickers_streams(self):
        '''
        doi kieu du lieu trong cac hang chua doi tuong {}
        them vao cot
        '''
        self.df = self.df.select(
            col("explode_message").getField("Last_price").alias("Last_price").cast(FloatType()),
            date_format(from_unixtime(col("explode_message").getField("Statistics_close_time") / 1000), "yyyy-MM-dd HH:mm:ss.SSS").alias("Statistics_close_time"),
            date_format(from_unixtime(col("explode_message").getField("Event_time") / 1000), "yyyy-MM-dd HH:mm:ss.SSS").alias("Event_time"),
            col("explode_message").getField("First_trade_ID").alias("First_trade_ID").cast(IntegerType()),
            col("explode_message").getField("High_price").alias("High_price").cast(FloatType()),
            col("explode_message").getField("Low_price").alias("Low_price").cast(FloatType()),
            col("explode_message").getField("Last_trade_ID").alias("Last_trade_ID").cast(IntegerType()),
            col("explode_message").getField("Total_number_of_trades").alias("Total_number_of_trades").cast(IntegerType()),
            col("explode_message").getField("Open_price").alias("Open_price").cast(FloatType()),
            date_format(from_unixtime(col("explode_message").getField("Statistics_open_time") / 1000), "yyyy-MM-dd HH:mm:ss.SSS").alias("Statistics_open_time"),
            col("explode_message").getField("Price_change_percent").alias("Price_change_percent").cast(FloatType()),
            col("explode_message").getField("Price_change").alias("Price_change").cast(FloatType()),
            col("explode_message").getField("Total_traded_quote_asset_volume").alias("Total_traded_quote_asset_volume").cast(FloatType()),
            col("explode_message").getField("Last_quantity").alias("Last_quantity").cast(FloatType()),
            col("explode_message").getField("Symbol").alias("Symbol").cast(StringType()),
            col("explode_message").getField("Total_traded_base_asset_volume").alias("Total_traded_base_asset_volume").cast(FloatType()),
            col("explode_message").getField("Weighted_average_price").alias("Weighted_average_price").cast(FloatType())
        )

    def format_message_aggtrade(self):
        '''
        doi kieu du lieu trong cac hang chua doi tuong {}
        them vao cot
        '''
        self.df = self.df.select(date_format(from_unixtime(col("structed_message").getField("Event_time") / 1000), "yyyy-MM-dd HH:mm:ss.SSS").alias("Event_time"),
            col("structed_message").getField("Aggregate_trade_ID").alias("Aggregate_trade_ID").cast(LongType()),
            col("structed_message").getField("Symbol").alias("Symbol").cast(StringType()),
            col("structed_message").getField("Price").alias("Price").cast(FloatType()),
            col("structed_message").getField("Quantity").alias("Quantity").cast(FloatType()),
            col("structed_message").getField("First_trade_ID").alias("First_trade_ID").cast(LongType()),
            col("structed_message").getField("Last_trade_ID").alias("Last_trade_ID").cast(LongType()),
            date_format(from_unixtime(col("structed_message").getField("Trade_time") / 1000), "yyyy-MM-dd HH:mm:ss.SSS").alias("Trade_time")
        )

    def format_message_candlestick(self):
        '''
        doi kieu du lieu trong cac hang chua doi tuong {}
        them vao cot
        '''
        self.df = self.df.select(date_format(from_unixtime(col("structed_message").getField("Event_time") / 1000), "yyyy-MM-dd HH:mm:ss.SSS").alias("Event_time"),
                col("structed_message").getField("Symbol").alias("Symbol").cast(StringType()),
                date_format(from_unixtime(col("structed_message").getField("k").getField("Kline_start_time") / 1000), "yyyy-MM-dd HH:mm:ss.SSS").alias("Kline_start_time"),
                date_format(from_unixtime(col("structed_message").getField("k").getField("Kline_close_time") / 1000), "yyyy-MM-dd HH:mm:ss.SSS").alias("Kline_close_time"),
                col("structed_message").getField("k").getField("Time_interval").alias("Time_interval").cast(StringType()),
                col("structed_message").getField("k").getField("First_trade_ID").alias("First_trade_ID").cast(LongType()),
                col("structed_message").getField("k").getField("Last_trade_ID").alias("Last_trade_ID").cast(LongType()),
                col("structed_message").getField("k").getField("Open_price").alias("Open_price").cast(FloatType()),
                col("structed_message").getField("k").getField("Close_price").alias("Close_price").cast(FloatType()),
                col("structed_message").getField("k").getField("High_price").alias("High_price").cast(FloatType()),
                col("structed_message").getField("k").getField("Low_price").alias("Low_price").cast(FloatType()),
                col("structed_message").getField("k").getField("Base_asset_volume").alias("Base_asset_volume").cast(FloatType()),
                col("structed_message").getField("k").getField("Number_of_trades").alias("Number_of_trades").cast(FloatType()),
                col("structed_message").getField("k").getField("Is_this_kline_closed").alias("Is_this_kline_closed").cast(BooleanType()),
                col("structed_message").getField("k").getField("Quote_asset_volume").alias("Quote_asset_volume").cast(FloatType()),
                col("structed_message").getField("k").getField("Taker_buy_base_asset_volume").alias("Taker_buy_base_asset_volume").cast(FloatType()),
                col("structed_message").getField("k").getField("Taker_buy_quote_asset_volume").alias("Taker_buy_quote_asset_volume").cast(FloatType())
                )

    def write_to_mysql(self, df, epoch_id, table_name):
        df.write \
            .format("jdbc") \
            .option("url", _env.CONNECTION_STRING_MYSQL) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", _env.USERNAME_MYSQL) \
            .option("password", _env.PASSWORD_MYSQL) \
            .mode("append") \
            .save()

    def write_stream_console(self):
        query = self.df.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .option("truncate", "false") \
                    .start()
        return query

    def write_stream_to_mysql(self, table_name):
        query = self.df.writeStream \
                    .foreachBatch(lambda df, epoch_id: self.write_to_mysql(df, epoch_id, table_name)) \
                    .outputMode("append") \
                    .start()
        return query

if __name__ == "__main__":
    # consumer_1 = kafkaConsumer(_env.TOPIC_AGGTRADE, _env.CONSUMER_GROUP)
    # consumer_1.print_message()
    
    ss = SparkSession.builder \
        .appName("spark consumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-java:8.0.30") \
        .master("local[*]") \
        .getOrCreate()

    spark_consumer_market_price = sparkConsumer(ss)
    spark_consumer_aggtrade = sparkConsumer(ss)
    spark_consumer_candlestick = sparkConsumer(ss)

    # xu ly message tu topic crypto-price
    spark_consumer_market_price.read_stream(_env.TOPIC_CRYPTO_PRICE)
    spark_consumer_market_price.replace_key(_env.REPLACEMENTS_TICKERS_STREAMS)
    spark_consumer_market_price.structed_message(_env.SCHEMA_TICKERS_STREAMS)
    spark_consumer_market_price.explode_message()
    spark_consumer_market_price.format_message_tickers_streams()
    # query_spark_consumer_market_price = spark_consumer_market_price.write_stream_console()
    query_spark_consumer_market_price = spark_consumer_market_price.write_stream_to_mysql(_env.TABLE_NAME_CRYPTO_PRICE)

    # xu ly message tu topic aggtrade
    spark_consumer_aggtrade.read_stream(_env.TOPIC_AGGTRADE)
    spark_consumer_aggtrade.replace_key(_env.REPLACEMENTS_AGGTRADE)
    spark_consumer_aggtrade.structed_message(_env.SCHEMA_AGGTRADE)
    spark_consumer_aggtrade.format_message_aggtrade()
    # query_spark_consumer_aggtrade = spark_consumer_aggtrade.write_stream_console()
    query_spark_consumer_aggtrade = spark_consumer_aggtrade.write_stream_to_mysql(_env.TABLE_NAME_AGGTRADE)

    # xu ly message tu topic candlestick
    spark_consumer_candlestick.read_stream(_env.TOPIC_CANDLESTICK)
    spark_consumer_candlestick.replace_key(_env.REPLACEMENTS_CANDLESTICK)
    spark_consumer_candlestick.structed_message(_env.SCHEMA_CANDLESTICK)
    spark_consumer_candlestick.format_message_candlestick()
    # query_spark_consumer_candlestick = spark_consumer_candlestick.write_stream_console()
    query_spark_consumer_candlestick = spark_consumer_candlestick.write_stream_to_mysql(_env.TABLE_NAME_CANDLESTICK)

    query_spark_consumer_market_price.awaitTermination()
    query_spark_consumer_aggtrade.awaitTermination()
    query_spark_consumer_candlestick.awaitTermination()
