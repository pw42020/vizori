import sys
import os
from pathlib import Path

from dotenv import load_dotenv
from deltalake import DeltaTable
import duckdb
import pandas as pd

ROOT_PATH = Path(__file__).parent

load_dotenv()

conn = duckdb.connect()

conn.execute(f"""
INSTALL httpfs;
LOAD httpfs;
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    ENDPOINT '{os.environ['S3_ENDPOINT']}',
    KEY_ID '{os.environ['ACCESS_KEY']}',
    SECRET '{os.environ["SECRET_KEY"]}',
    REGION 'us-east-1'
);
""")

# open file given from sys.argv
df = pd.read_csv(sys.argv[1])

# add the dataframe to the delta table using duckdb
conn.execute(f"""
CREATE OR REPLACE TABLE delta_table AS
SELECT * FROM df
""")