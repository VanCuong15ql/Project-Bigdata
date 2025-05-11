import logging

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType

import udfs

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.extracted_recruit (
        id TEXT PRIMARY KEY,
        company_name TEXT,
        framework_platforms LIST<TEXT>,
        languages LIST<TEXT>,
        design_patterns LIST<TEXT>,
        knowledges LIST<TEXT>,
        salaries LIST<INT>);
    """)

    print("Table created successfully!")

def create_table_raw(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.raw_recruit (
        id TEXT PRIMARY KEY,
        name TEXT,
        mo_ta_cong_viec TEXT,
        yeu_cau_cong_viec TEXT,
        quyen_loi TEXT,
        cach_thuc_ung_tuyen TEXT);
    """)

    print("Table_raw created successfully!")

# def insert_data(session, **kwargs):
#     print("inserting data...")

#     id = kwargs.get('ID')
#     companyName = kwargs.get('CompanyName')
#     frameworkPlatforms = kwargs.get('FrameworkPlatforms')
#     languages = kwargs.get('Languages')
#     designPatterns = kwargs.get('DesignPatterns')
#     knowledges = kwargs.get('Knowledges')
#     salaries = kwargs.get('Salaries')

#     try:
#         session.execute("""
#             INSERT INTO spark_streams.extracted_recruit(
#                     id, companyName, frameworkPlatforms, languages, designPatterns, knowledges, salaries
#                 )
#                 VALUES (%s, %s, %s, %s, %s, %s, %s)
#         """, (id, companyName, frameworkPlatforms, languages, designPatterns, knowledges, salaries))
#         logging.info("Data inserted for" + str(id) + " " + str(companyName))

#     except Exception as e:
#         logging.error('could not insert data due to ' + str(e))


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('Spark_Streaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error("Couldn't create the spark session due to exception " + str(e))

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
        logging.warning("kafka dataframe could not be created because: " + str(e))

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error("Could not create cassandra connection due to " + str(e))
        return None


def create_selection_df_from_kafka(spark_df):
    schema= StructType([
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

def transform_data(df_raw):
    # return df_raw
    df = df_raw.dropna()
    extracted_recruit_df = df.select(
            df['id'],
            df["name"].alias("company_name"),
            udfs.extract_framework_plattform("mo_ta_cong_viec","yeu_cau_cong_viec").alias("framework_platforms"),
            udfs.extract_language("mo_ta_cong_viec","yeu_cau_cong_viec").alias("languages"),
            udfs.extract_design_pattern("mo_ta_cong_viec","yeu_cau_cong_viec").alias("design_patterns"),
            udfs.extract_knowledge("mo_ta_cong_viec","yeu_cau_cong_viec").alias("knowledges"),
            udfs.normalize_salary("quyen_loi").alias("salaries")
            )

    extracted_recruit_df.printSchema()

    return extracted_recruit_df

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)
            transformed_df = transform_data(selection_df)

            session = create_cassandra_connection()

            if session is not None:
                create_keyspace(session)
                create_table(session)
                # create_table_raw(session)

                logging.info("Streaming is being started...")

                # insert_data(session)
                streaming_query = (transformed_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .option('keyspace', 'spark_streams')
                                .option('table', 'extracted_recruit')
                                .start())

                streaming_query.awaitTermination()
        else:
            print("ERROR: spard_df")
