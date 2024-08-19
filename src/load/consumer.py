import kafka
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import _env


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
    def __init__(self, app_name) -> None:
        self.ss = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-java:8.0.30") \
            .master("local[*]") \
            .getOrCreate()

        self.df = None

    def read_stream(self):
        self.df = self.ss.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", _env.BROKER_KAFKA_IP) \
                .option("subscribe", "crypto-price") \
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
        message co dang chuoi [{}, {},...]
        dung from_json phan tich cu phap {}, chuyen thanh cau truc du lieu duoc xac dinh trong schema
        '''
        self.df = self.df.withColumn("structed_message", from_json(self.df["value"], ArrayType(schema))).select("structed_message")

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

    def write_to_mysql(self, df, epoch_id):
        df.write \
            .format("jdbc") \
            .option("url", _env.CONNECTION_STRING_MYSQL) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", _env.TABLE_NAME_CRYPTO_PRICE) \
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
        query.awaitTermination()

    def write_stream_to_mysql(self):
        query = self.df.writeStream \
                    .foreachBatch(self.write_to_mysql) \
                    .outputMode("append") \
                    .start()
        query.awaitTermination()



# consumer_1 = kafkaConsumer(_env.TOPIC_CRYPTO_PRICE, _env.CONSUMER_GROUP)
# consumer_1.print_message()

spark_consumer_1 = sparkConsumer("spark consumer")
spark_consumer_1.read_stream()
spark_consumer_1.replace_key(_env.REPLACEMENTS_TICKERS_STREAMS)
spark_consumer_1.structed_message(_env.SCHEMA_TICKERS_STREAMS)
spark_consumer_1.explode_message()
spark_consumer_1.format_message_tickers_streams()
spark_consumer_1.write_stream_to_mysql()
