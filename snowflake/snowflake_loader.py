import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# LOAD ENV VARIABLES
load_dotenv()

required_vars = [
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]

for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"Missing environment variable: {var}")

# CONNECT TO SNOWFLAKE

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
    CRYPTO_ID STRING PRIMARY KEY,
    NAME STRING,
    SYMBOL STRING
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS DIM_DATE (
    DATE_ID NUMBER PRIMARY KEY,
    FULL_DATE DATE,
    YEAR NUMBER,
    MONTH NUMBER,
    DAY NUMBER
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS FACT_MARKET (
    FACT_ID NUMBER PRIMARY KEY,

    CRYPTO_ID STRING NOT NULL,
    DATE_ID NUMBER NOT NULL,

    CURRENT_PRICE FLOAT,
    HIGH_24H FLOAT,
    LOW_24H FLOAT,
    TOTAL_VOLUME FLOAT,
    MARKET_CAP FLOAT,
    PRICE_CHANGE_PERCENTAGE_24H FLOAT,

    CONSTRAINT UK_CRYPTO_DATE
        UNIQUE (CRYPTO_ID, DATE_ID)
)
""")

print("Tables created successfully")

# FILE PATHS

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
        print(f"{table_name}: Empty file")
        return

    # Normalize column names
    df.columns = df.columns.str.upper()

    # Remove unnecessary columns
    df = df.drop(
        columns=["ID", "INDEX", "UNNAMED: 0"],
        errors="ignore"
    )

    # DIM_CRYPTO

    if table_name == "DIM_CRYPTO":

        df = df[["CRYPTO_ID", "NAME", "SYMBOL"]]

        df = df.drop_duplicates(
            subset=["CRYPTO_ID"]
        )

    # DIM_DATE

    elif table_name == "DIM_DATE":

        df["FULL_DATE"] = pd.to_datetime(
            df["FULL_DATE"],
            errors="coerce"
        ).dt.date

        df = df[
            [
                "DATE_ID",
                "FULL_DATE",
                "YEAR",
                "MONTH",
                "DAY"
            ]
        ]

        df = df.drop_duplicates(
            subset=["DATE_ID"]
        )

    # FACT_MARKET

    elif table_name == "FACT_MARKET":

        df = df[
            [
                "FACT_ID",
                "CRYPTO_ID",
                "DATE_ID",
                "CURRENT_PRICE",
                "HIGH_24H",
                "LOW_24H",
                "TOTAL_VOLUME",
                "MARKET_CAP",
                "PRICE_CHANGE_PERCENTAGE_24H"
            ]
        ]

        df = df.drop_duplicates(
            subset=["CRYPTO_ID", "DATE_ID"]
        )

    df = df.dropna(how="all")

    print(f"\nLoading {table_name}")
    print(df.head())

    try:

        # Empty table before loading
        cur.execute(f"TRUNCATE TABLE {table_name}")

        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name,
            auto_create_table=False
        )

        if success:
            print(f"Loaded {nrows} rows into {table_name}")
        else:
            print(f"Failed loading {table_name}")

    except Exception as e:
        print(f"Error loading {table_name}: {e}")

# LOAD TABLES IN ORDER

load_table(
    "DIM_CRYPTO",
    files["DIM_CRYPTO"]
)

load_table(
    "DIM_DATE",
    files["DIM_DATE"]
)

load_table(
    "FACT_MARKET",
    files["FACT_MARKET"]
)

# VALIDATION
cur.execute("SELECT COUNT(*) FROM DIM_CRYPTO")
print("DIM_CRYPTO:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM DIM_DATE")
print("DIM_DATE:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM FACT_MARKET")
print("FACT_MARKET:", cur.fetchone()[0])

# CLOSE CONNECTION
cur.close()
conn.close()

print("Snowflake Load Finished Successfully")