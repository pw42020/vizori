from pyspark.sql import SparkSession
import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .appName("DeltaLakeS3Example") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", "fMXQlnQyCPVAH0VwDpxJ") \
    .config("spark.hadoop.fs.s3a.secret.key", "Q8xcaHLlMSue6L88zexQHAfBhJqzwXj6QUavyzWY") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 28)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

s3_path = "s3a://test/titanic/pyspark"
# df.write.format("delta").mode("overwrite").save(s3_path)

df_read = spark.read.format("delta").load("s3a://test/titanic/train")
df_read.show()
