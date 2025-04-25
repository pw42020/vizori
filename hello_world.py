import pyspark
from pyspark import SparkConf
import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
ROOT_PATH = Path(__file__).parent

conf = SparkConf().setAppName("AddDataToMinio")
jars = [str(ROOT_PATH / "jars" / jar) for jar in os.listdir(ROOT_PATH / "jars") if jar.endswith(".jar")]
sc = pyspark.SparkContext(conf=conf)

spark = SparkSession(sc).builder.appName("AddDataToMinio") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .appName("AddDataToMinio").getOrCreate()
# .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
# .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", 'fMXQlnQyCPVAH0VwDpxJ')
hadoop_conf.set("fs.s3a.secret.key", 'Q8xcaHLlMSue6L88zexQHAfBhJqzwXj6QUavyzWY')
hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# write csv as csv to minio
path: str = f"s3://{os.environ['BUCKET_NAME']}/titantic/train.csv"
print(path)

# read from csv file
df = spark.read.csv(path, header=True, inferSchema=True)
# show the dataframe
df.show()