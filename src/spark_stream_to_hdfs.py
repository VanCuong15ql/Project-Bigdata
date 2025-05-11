import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkStreamToHDFS') \
            .master("spark://spark-master:7077")\
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")\
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error("Couldn't create the spark session due to exception" + str(e))

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'recruitment_information') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning("kafka dataframe could not be created because:" + str(e))

    return spark_df




def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("mo_ta_cong_viec", StringType(), False),
        StructField("yeu_cau_cong_viec", StringType(), False),
        StructField("quyen_loi", StringType(), False),
        StructField("cach_thuc_ung_tuyen", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)

            streaming_query = (selection_df.writeStream.format("json")
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .option('keyspace', 'spark_streams')
                                .option('path', 'hdfs://namenode:9000/data/raw/')
                                .start())
            streaming_query.awaitTermination()
            
        else:
            print("ERROR: spard_df")
