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
    .option("subscribe", bs_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()
# \
#     .withWatermark("timestamp", "10 seconds")

#    .option("maxOffsetsPerTrigger", "5") \
# .option("checkpointLocation", "./checkpoint")\

print("DISPLAY DATA BEGIN df")

# # Виведення даних на екран
displaying_df = df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .options(truncate=False) \
    .option("checkpointLocation", "./tmp/checkpoints-2") \
    .start() \
    .awaitTermination()

print("DISPLAY DATA END")

clean_df = df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*") \
    .drop('key', 'value')  \
    .withColumnRenamed("key_deserialized", "key") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .withColumn("timestamp", from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp")) \
    .withColumn("temperature", col("value_json.temperature")) \
    .withColumn("humidity", col("value_json.humidity")) \
    .withColumn("sensor", col("value_json.sensor")) \
    .drop("value_json", "value_deserialized")

print("DISPLAY DATA BEGIN clean_df")

# Виведення даних на екран
displaying_df = clean_df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "./tmp/checkpoints-3") \
    .start() \
    .awaitTermination()

print("DISPLAY DATA END")

# Групування даних за полем 'value' і підрахунок mean
mean_df = clean_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window("timestamp", "1 minutes","30 seconds"),'sensor') \
    .agg(avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg"))

print("DISPLAY DATA BEGIN mean_df")

# Виведення даних на екран
displaying_df = mean_df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "./tmp/checkpoints-4") \
    .start() \
    .awaitTermination()

print("DISPLAY DATA END")

alerts_df = spark.read.load("alerts_conditions.csv", format="csv", inferSchema="true", header="true")


mean_df.printSchema()
alerts_df.printSchema()

joined_df = mean_df.join(alerts_df)

rint("DISPLAY DATA BEGIN joined_df")

# Виведення даних на екран
displaying_df = joined_df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .options(truncate=False) \
    .option("checkpointLocation", "./tmp/checkpoints-51") \
    .start() \
    .awaitTermination()

print("DISPLAY DATA END")


alerts_result_df = joined_df.filter(((joined_df.temperature_min>joined_df.t_avg) | (joined_df.t_avg>joined_df.temperature_max)) | \
                                     ((joined_df.humidity_min>joined_df.h_avg) | (joined_df.h_avg>joined_df.humidity_max))) \
    .drop("temperature_min","temperature_max","humidity_min","humidity_max","id")


print("DISPLAY DATA BEGIN alerts_result_df")

# Виведення даних на екран
displaying_df = alerts_result_df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .options(truncate=False) \
    .option("checkpointLocation", "./tmp/checkpoints-5") \
    .start() \
    .awaitTermination()

print("DISPLAY DATA END")

alerts_result_df=alerts_result_df.withColumn('key',expr("uuid()"))

prepare_to_kafka_df = alerts_result_df.select(
    col("key"),
    to_json(struct(col("window"),col("t_avg"),col("h_avg"),col("code"), col("message"))).alias("value")
)

print("DISPLAY DATA BEGIN prepare_to_kafka_df")

# Виведення даних на екран
displaying_df = prepare_to_kafka_df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .options(truncate=False) \
    .option("checkpointLocation", "./tmp/checkpoints-6") \
    .start() \
    .awaitTermination()

print("DISPLAY DATA END")

print("WRITE TO KAFKA")

query = prepare_to_kafka_df.writeStream \
    .trigger(processingTime='30 seconds') \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", alert_topic_name) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
    .option("checkpointLocation", "./tmp/checkpoints-7") \
    .start() \
    .awaitTermination()
