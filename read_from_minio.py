import duckdb
import os
from delta import DeltaTable
from dotenv import load_dotenv

load_dotenv()
os.environ["AWS_EC2_METADATA_DISABLED"] = "true" # Necessary due to AWS IMDS requests timing out

conn = duckdb.connect()

conn.execute(f"""
INSTALL httpfs;
LOAD httpfs;
INSTALL delta;
LOAD delta;
CREATE OR REPLACE SECRET secret (
    TYPE S3,
    REGION 'us-east-1',
    USE_SSL 'false',
    URL_STYLE 'path',
    ENDPOINT '{os.environ['S3_ENDPOINT']}',
    KEY_ID '{os.environ['AWS_ACCESS_KEY_ID']}',
    SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}'
);""")

print(f"s3://{os.environ['BUCKET_NAME']}/titantic/train")
df = conn.execute(f"""
SELECT *
FROM read_csv_auto('/media/pwalsh/Drive/minio/titanic/train.csv');
""").df()

# show the dataframe
print(df.head())