import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

os.environ["AWS_EC2_METADATA_DISABLED"] = "true"  # Necessary due to AWS IMDS requests timing out


def query_minio_duckdb(bucket_name, object_name):
    try:
        # Connect to DuckDB and read Parquet files
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
        conn.execute(f"CREATE TABLE data AS SELECT * FROM delta_scan('s3://{bucket_name}/{object_name}')")

        # Number of Rows in data
        total_rows_result = conn.execute("SELECT COUNT(*) AS TotalRows FROM data")
        total_rows = total_rows_result.fetchone()[0]
        print(f"Number of rows in data: {total_rows}")

        # Make a list of columns
        columns_result = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'data'")
        columns = columns_result.fetchall()

        # Number of nulls in each column
        for column in columns:
            column_name = column[0]
            result = conn.execute(f"SELECT COUNT(*) FROM data WHERE {column_name} IS NULL")
            null_count = result.fetchone()[0]
            print(f"Number of nulls in '{column_name}': {null_count}")

        # Summary statistics for numeric columns
        numeric_columns_result = conn.execute("SELECT column_name FROM information_schema.columns"
                                              " WHERE table_name = 'data' "
                                              "AND data_type IN ('BIGINT','INTEGER','DOUBLE')")
        numeric_columns = numeric_columns_result.fetchall()
        for column in numeric_columns:
            column_name = column[0]
            result = conn.execute(
                f"SELECT MIN({column_name}), MAX({column_name}), AVG({column_name}), STDDEV({column_name}) FROM data")
            min_val, max_val, avg_val, stddev_val = result.fetchone()
            print(f"Summary statistics for '{column_name}':")
            print(f"  Min: {min_val}")
            print(f"  Max: {max_val}")
            print(f"  Average: {avg_val}")
            print(f"  Standard Deviation: {stddev_val}")

        # Clean Up
        conn.close()
    except Exception as e:
        print(f"Error: {e}")


# Replace 'ducknest' and 'insects.parquet' with your bucket and object names
query_minio_duckdb('test/titanic', 'train')