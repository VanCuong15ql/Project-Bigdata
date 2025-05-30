# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, trim, from_unixtime
from pyspark.sql.types import *

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaToElasticsearchJob") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.9.0") \
    .config("es.nodes", "elasticsearch") \
    .config("es.port", "9200") \
    .config("es.nodes.wan.only", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Định nghĩa schema cho JSON
job_schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("chuyen_mon", StringType()),
    StructField("mo_ta_cong_viec", StringType()),
    StructField("yeu_cau_cong_viec", StringType()),
    StructField("quyen_loi", StringType()),
    StructField("dia_diem_lam_viec", StringType()),
    StructField("thoi_gian_lam_viec", StringType()),
    StructField("cach_thuc_ung_tuyen", StringType())
])

# Đọc từ Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "recruitment_information") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), job_schema).alias("data")) \
    .select("data.*")
print(df_parsed)    
# Làm sạch đơn giản (có thể mở rộng thêm)
df_clean = df_parsed.withColumn("mo_ta_cong_viec", regexp_replace(col("mo_ta_cong_viec"), r"\n+", " ")) \
    .withColumn("yeu_cau_cong_viec", regexp_replace(col("yeu_cau_cong_viec"), r"\n+", " ")) \
    .withColumn("quyen_loi", regexp_replace(col("quyen_loi"), r"\n+", " ")) \
    .withColumn("name", trim(col("name"))) \
    .withColumn("chuyen_mon", trim(col("chuyen_mon")))\
    .withColumn("id", from_unixtime(col("ngay_dang_tin").cast("double"),"yyyy-MM-dd")) \
    
# check df_clean
df_clean.printSchema()

def write_to_elasticsearch(batch_df, batch_id):
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "jobs") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .mode("append") \
        .save()
query = df_clean.writeStream \
    .foreachBatch(write_to_elasticsearch) \
    .option("checkpointLocation", "/tmp/spark_checkpoint_jobs") \
    .start()
# check query status
if query.isActive:
    print("Streaming query is active.")
print("Streaming query started, waiting for termination...")
query.awaitTermination()
