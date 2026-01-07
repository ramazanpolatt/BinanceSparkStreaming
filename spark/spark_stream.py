import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum, count, date_format, round
from pyspark.sql.types import StructType, StringType, LongType

s3_path = "s3a://spark-crypto/binance/"
checkpoint_path = "s3a://spark-crypto/checkpoints/"

access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.sql.caseSensitive", "true") \
    .config("spark.ui.prometheus.enabled", "true") \
    .config("spark.metrics.namespace", "binance_stream") \
    .config("spark.metrics.conf.*.sink.prometheusServlet.class", "org.apache.spark.metrics.sink.PrometheusServlet") \
    .config("spark.metrics.conf.*.sink.prometheusServlet.path", "/metrics/prometheus") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("e", StringType()) \
    .add("E", LongType()) \
    .add("s", StringType()) \
    .add("q", StringType()) \
    .add("p", StringType())

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "binance_trades")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data").getField("e").alias("event_type"),
    col("data").getField("E").alias("event_time_raw"),
    col("data").getField("s").alias("symbol"),
    col("data").getField("q").cast("float").alias("quantity"),
    col("data").getField("p").cast("double").alias("price")
)

parsed = parsed.withColumn(
    "event_time",
    (col("event_time_raw") / 1000).cast("timestamp")
)

windowed_df = parsed.withWatermark("event_time", "10 seconds") \
    .groupBy(window(col('event_time'), '1 minute'), col("symbol")) \
    .agg(avg("price").alias("avg_price"),
         sum("quantity").alias('total_volume'),
         count("*").alias('trade_count'))
windowed_df = windowed_df.withColumn("avg_price", round(windowed_df.avg_price, 2))

final_df = windowed_df.select(
    col("symbol"),
    col("avg_price"),
    col("total_volume"),
    col("trade_count"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    date_format(col("window.start"), "yyyy-MM-dd-HH").alias("hour_partition")
)

# Use for debugging
# console_query = final_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .trigger(processingTime='10 seconds') \
#     .queryName("console_output") \
#     .start()

query = final_df.writeStream \
    .outputMode("append") \
    .partitionBy("hour_partition") \
    .format("delta") \
    .option("path", s3_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime='1 minute') \
    .queryName("s3_output") \
    .start()

spark.streams.awaitAnyTermination()
