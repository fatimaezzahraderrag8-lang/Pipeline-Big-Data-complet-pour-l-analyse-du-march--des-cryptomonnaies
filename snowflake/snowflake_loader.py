import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

# CHECK ENV VARIABLES
required_vars = [
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]

for v in required_vars:
    if not os.getenv(v):
        raise ValueError(f"Missing env var: {v}")

# CONNECTION
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

cur = conn.cursor()
print("Connected to Snowflake")

# CREATE TABLES
cur.execute("""
CREATE TABLE IF NOT EXISTS DIM_CRYPTO (
    CRYPTO_ID STRING,
    NAME STRING,
    SYMBOL STRING
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS DIM_DATE (
    DATE_ID NUMBER,
    FULL_DATE DATE,
    YEAR NUMBER,
    MONTH NUMBER,
    DAY NUMBER
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS FACT_MARKET (
    FACT_ID NUMBER,
    CRYPTO_ID STRING,
    DATE_ID NUMBER,
    CURRENT_PRICE FLOAT,
    HIGH_24H FLOAT,
    LOW_24H FLOAT,
    TOTAL_VOLUME FLOAT,
    MARKET_CAP FLOAT,
    PRICE_CHANGE_PERCENTAGE_24H FLOAT
)
""")

print("Tables created")

# PATHS
BASE_PATH = "/opt/airflow/project/gold"

files = {
    "DIM_CRYPTO": os.path.join(BASE_PATH, "dim_crypto.parquet"),
    "DIM_DATE": os.path.join(BASE_PATH, "dim_date.parquet"),
    "FACT_MARKET": os.path.join(BASE_PATH, "fact_market.parquet"),
}

# LOAD FUNCTION
def load_table(table_name, file_path):

    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    df = pd.read_parquet(file_path)

    if df.empty:
        print(f"EMPTY FILE: {file_path}")
        return

    print(f"\n{table_name} RAW DATA")
    print(df.head())

    # CLEANING
    df.columns = df.columns.str.upper()
    df = df.drop(columns=["ID"], errors="ignore")

    if "FULL_DATE" in df.columns:
        df["FULL_DATE"] = pd.to_datetime(df["FULL_DATE"], errors="coerce").dt.date

    df = df.dropna(how="all")

    print(f"\n{table_name} CLEAN DATA")
    print(df.head())

    # LOAD TO SNOWFLAKE
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name
        )

        if success:
            print(f"Loaded {nrows} rows into {table_name}")
        else:
            print(f"Failed loading {table_name}")

    except Exception as e:
        print(f"ERROR in {table_name}: {e}")

# EXECUTION
for table, path in files.items():
    load_table(table, path)

# VALIDATION (FIXED)
cur.execute("SELECT COUNT(*) FROM DIM_CRYPTO")
print("DIM_CRYPTO:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM DIM_DATE")
print("DIM_DATE:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM FACT_MARKET")
print("FACT_MARKET:", cur.fetchone()[0])

cur.close()
conn.close()

print("DONE")