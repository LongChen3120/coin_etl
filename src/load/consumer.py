import sys
sys.path.append("../src")

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import config


class sparkConsumer():
    connection_string = config.CONNECTION_STRING_MYSQL
    def __init__(self, ss) -> None:
        self.ss = ss
        self.df = None

    def read_stream(self, topic_name):
        self.df = self.ss.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", config.BROKER_KAFKA_IP) \
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
        self.df = self.df.withColumn("value", col("value").cast("string"))
        self.df = self.df.withColumn("structed_message", from_json(self.df["value"], schema)).select("structed_message")

    def explode_message(self):
        '''
        chuyen du lieu dang mang [{}, {},..] sang moi hang mot doi tuong {}
        '''
        self.df = self.df.withColumn("explode_message", explode(self.df["structed_message"])).select("explode_message")

    def format_message_candlestick(self):
        '''
        doi kieu du lieu trong cac hang chua doi tuong {}
        them vao cot
        '''
        self.df = self.df.select(col("structed_message").getField("Symbol").alias("symbol").cast(StringType()),
                date_format(from_unixtime(col("structed_message").getField("Kline_start_time") / 1000), "yyyy-MM-dd HH:mm:ss").alias("date"),
                col("structed_message").getField("Open_price").alias("open").cast(FloatType()),
                col("structed_message").getField("Close_price").alias("close").cast(FloatType()),
                col("structed_message").getField("High_price").alias("high").cast(FloatType()),
                col("structed_message").getField("Low_price").alias("low").cast(FloatType()),
                col("structed_message").getField("Volume").alias("volume").cast(FloatType()),
                )

    def config_write_to_mysql(self, df, epoch_id, table_name):
        df.write \
            .format("jdbc") \
            .option("url", config.CONNECTION_STRING_MYSQL) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", config.USERNAME_MYSQL) \
            .option("password", config.PASSWORD_MYSQL) \
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
                    .foreachBatch(lambda df, epoch_id: self.config_write_to_mysql(df, epoch_id, table_name)) \
                    .outputMode("append") \
                    .start()
        return query

if __name__ == "__main__":
    # consumer_1 = kafkaConsumer(config.TOPIC_AGGTRADE, config.CONSUMER_GROUP)
    # consumer_1.print_message()
    
    ss = SparkSession.builder \
        .appName("spark consumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-java:8.0.30") \
        .master("local[*]") \
        .getOrCreate()

    # futures
    spark_consumer_candlestick = sparkConsumer(ss)
    # spot
    spark_consumer_topic_candle_spot = sparkConsumer(ss)


    # xu ly message tu topic candle-futures
    spark_consumer_candlestick.read_stream(config.TOPIC_CANDLE)
    # spark_consumer_candlestick.replace_key(config.REPLACEMENTS_CANDLESTICK)
    spark_consumer_candlestick.structed_message(config.SCHEMA_CANDLESTICK)
    spark_consumer_candlestick.format_message_candlestick()
    query_spark_consumer_candlestick = spark_consumer_candlestick.write_stream_console()
    query_spark_consumer_candlestick = spark_consumer_candlestick.write_stream_to_mysql(config.TABLE_NAME_CANDLE)


    # xu ly message tu topic candle-spot
    spark_consumer_topic_candle_spot.read_stream(config.TOPIC_CANDLE_SPOT)
    spark_consumer_topic_candle_spot.structed_message(config.SCHEMA_CANDLESTICK)
    spark_consumer_topic_candle_spot.format_message_candlestick()
    query_spark_consumer_topic_candle_spot = spark_consumer_topic_candle_spot.write_stream_console()
    query_spark_consumer_topic_candle_spot = spark_consumer_topic_candle_spot.write_stream_to_mysql(config.TABLE_NAME_CANDLE_SPOT)

    query_spark_consumer_candlestick.awaitTermination()
    query_spark_consumer_topic_candle_spot.awaitTermination()
