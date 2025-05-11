# coding=utf-8
import pyspark
from pyspark.sql.functions import *
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
    StructField("mo_ta_cong_viec", StringType(), False),
    StructField("yeu_cau_cong_viec", StringType(), False),
    StructField("quyen_loi", StringType(), False),
    StructField("cach_thuc_ung_tuyen", StringType(), False)
])

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('Spark_transformation') \
            .master("spark://spark-master:7077")\
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")\
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

        extracted_recruit_df = df_raw.select(df_raw["name"].alias("CompanyName"),
            udfs.extract_framework_plattform("mo_ta_cong_viec","yeu_cau_cong_viec").alias("FrameworkPlattforms"),
            udfs.extract_language("mo_ta_cong_viec","yeu_cau_cong_viec").alias("Languages"),
            udfs.extract_design_pattern("mo_ta_cong_viec","yeu_cau_cong_viec").alias("DesignPatterns"),
            udfs.extract_knowledge("mo_ta_cong_viec","yeu_cau_cong_viec").alias("Knowledges"),
            udfs.normalize_salary("quyen_loi").alias("Salaries")
            )
        extracted_recruit_df.cache()

        ##========save extracted_recruit_df to hdfs========================
        extracted_recruit_df.write\
            .format("json")\
            .mode("overwrite")\
            .save("hdfs://namenode:9000/data/extracted_data/recruit.json")
