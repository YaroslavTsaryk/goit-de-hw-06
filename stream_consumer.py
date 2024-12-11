from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from pyspark.sql import SparkSession
from configs import kafka_config
import os
from pyspark.sql.functions import window, avg, expr
import uuid

# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'


my_name = "YT"
bs_topic_name = f'{my_name}_building_sensors'
alert_topic_name = f'{my_name}_spark_streaming_out'


# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .config("forceDeleteTempCheckpointLocation", True)
         .getOrCreate())


# Визначення схеми для JSON
json_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("sensor", IntegerType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])


# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
# maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("failOnDataLoss", "false") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", alert_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

print("DISPLAY DATA BEGIN df")

# # Виведення даних на екран
displaying_df = df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .options(truncate=False) \
    .option("checkpointLocation", "./tmp/checkpoints-22") \
    .start() \
    .awaitTermination()

print("DISPLAY DATA END")