# coding=utf-8
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.functions import trim, regexp_replace, col
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from operator import add
import sys,os
from pyspark.sql.types import *

import logging
import udfs

schema_raw= StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("chuyen_mon", StringType(), False),
    StructField("mo_ta_cong_viec", StringType(), False),
    StructField("yeu_cau_cong_viec", StringType(), False),
    StructField("quyen_loi", StringType(), False),
    StructField("dia_diem_lam_viec", StringType(), False),
    StructField("thoi_gian_lam_viec", StringType(), False),      
    StructField("cach_thuc_ung_tuyen", StringType(), False)
])

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('Spark_transformation') \
            .master("spark://spark-master:7077")\
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error("Couldn't create the spark session due to exception " + str(e))

    return s_conn

if __name__ == "__main__":
    
    APP_NAME="spark_transformation"
    
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        df_raw = spark_conn.read.schema(schema_raw).json("hdfs://namenode:9000/data/raw/*.json")
        df_raw.printSchema()
        
        df_clean = df_raw.select(
            trim(regexp_replace(col("mo_ta_cong_viec"), "<[^>]*>", "")).alias("mo_ta_cong_viec"),
            trim(regexp_replace(col("chuyen_mon"), "<[^>]*>", "")).alias("chuyen_mon"),
        )
        # show data
        df_clean.cache()
        
        print(df_clean.count())
        # write to hdfs, format json
        df_clean.coalesce(1).write.mode("overwrite").json("hdfs://namenode:9000/data/cleaned/")
         

        
  
