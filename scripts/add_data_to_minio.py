import sys
import os
from pathlib import Path

from dotenv import load_dotenv
from deltalake import DeltaTable, write_deltalake
import duckdb
import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark
from delta import *
ROOT_PATH = Path(__file__).parent

load_dotenv()

conf = SparkConf().setAppName("AddDataToMinio")
jars = [str(ROOT_PATH / "jars" / jar) for jar in os.listdir(ROOT_PATH / "jars") if jar.endswith(".jar")]
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc).builder.appName("AddDataToMinio") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .appName("AddDataToMinio").getOrCreate()
# conf.set('spark.hadoop.fs.s3a.access.key','fMXQlnQyCPVAH0VwDpxJ')
# conf.set('spark.hadoop.fs.s3a.secret.key', 'Q8xcaHLlMSue6L88zexQHAfBhJqzwXj6QUavyzWY')
# add my jars in ROOT_PATH / jars as a list

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", 'fMXQlnQyCPVAH0VwDpxJ')
hadoop_conf.set("fs.s3a.secret.key", 'Q8xcaHLlMSue6L88zexQHAfBhJqzwXj6QUavyzWY')
hadoop_conf.set("fs.s3a.endpoint", os.environ['S3_ENDPOINT'])
hadoop_conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# open file given from sys.argv
df = pd.read_csv(sys.argv[1])
spark_df = spark.createDataFrame(df)

# write the dataframe to a delta table
spark_df.write.format("csv").mode("overwrite").save(f"s3a://{os.environ['BUCKET_NAME']}/titantic/train")
# builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
#     .config(conf=conf) \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
# add Class org.apache.hadoop.fs.s3a.S3AFileSystem 

# conn = duckdb.connect()
# 
# conn.execute(f"""
# INSTALL httpfs;
# LOAD httpfs;
# INSTALL delta;
# LOAD delta;
# -- CREATE OR REPLACE SECRET secret (
# --     TYPE s3,
# --     ENDPOINT '{os.environ['S3_ENDPOINT']}',
# --     KEY_ID '{os.environ['ACCESS_KEY']}',
# --     SECRET '{os.environ["SECRET_KEY"]}',
# --     REGION 'us-east-1'
# -- );
# """)

# spark_df = spark.createDataFrame(df)

# write the dataframe to a delta table
# set up storage options for Delta Lake

# storage_options = {
#     "AWS_REGION":'us-east-1',
#     'AWS_ACCESS_KEY_ID': os.environ['ACCESS_KEY'],
#     'AWS_SECRET_ACCESS_KEY': os.environ['SECRET_KEY'],
#     'AWS_ENDPOINT_URL': os.environ['S3_ENDPOINT'],
# }

# add the dataframe to the delta table using duckdb
# val = conn.execute(f"""
# CREATE OR REPLACE TABLE delta_table AS
# SELECT * FROM df
# """).df().dropna()

# quack = DeltaTable(f"s3a://{os.environ['BUCKET_NAME']}/titantic/train", storage_options=storage_options)
# 
# write_deltalake(quack, df)
