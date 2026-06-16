import os
import pandas as pd
import boto3
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

#  MINIO 
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

BUCKET = "gold"

# LOAD SILVER 
SILVER_FILE = "/opt/airflow/project/silver/crypto_silver.parquet"
df = pd.read_parquet(SILVER_FILE)

#  DIM_CRYPTO 
dim_crypto = df[["id", "name", "symbol"]].drop_duplicates()
dim_crypto.columns = ["CRYPTO_ID", "NAME", "SYMBOL"]

# DIM_DATE
dim_date = df[["full_date"]].drop_duplicates().copy()

dim_date["full_date"] = pd.to_datetime(dim_date["full_date"]).dt.date
dim_date = dim_date.sort_values("full_date").reset_index(drop=True)

dim_date["DATE_ID"] = range(1, len(dim_date) + 1)
dim_date["YEAR"] = pd.to_datetime(dim_date["full_date"]).dt.year
dim_date["MONTH"] = pd.to_datetime(dim_date["full_date"]).dt.month
dim_date["DAY"] = pd.to_datetime(dim_date["full_date"]).dt.day

dim_date = dim_date[["DATE_ID", "full_date", "YEAR", "MONTH", "DAY"]]
dim_date.columns = ["DATE_ID", "FULL_DATE", "YEAR", "MONTH", "DAY"]

#  FACT 
fact = df.copy()

fact["FACT_ID"] = range(1, len(fact) + 1)
fact["CRYPTO_ID"] = fact["id"]

date_map = dict(zip(dim_date["FULL_DATE"], dim_date["DATE_ID"]))
fact["DATE_ID"] = pd.to_datetime(fact["full_date"]).dt.date.map(date_map)

fact = fact[
    [
        "FACT_ID",
        "CRYPTO_ID",
        "DATE_ID",
        "current_price",
        "high_24h",
        "low_24h",
        "total_volume",
        "market_cap",
        "price_change_percentage_24h"
    ]
]

#  SAVE LOCAL FILES 
GOLD_PATH = "/opt/airflow/project/gold"
os.makedirs(GOLD_PATH, exist_ok=True)

dim_crypto.to_parquet(os.path.join(GOLD_PATH, "dim_crypto.parquet"), index=False)
dim_date.to_parquet(os.path.join(GOLD_PATH, "dim_date.parquet"), index=False)
fact.to_parquet(os.path.join(GOLD_PATH, "fact_market.parquet"), index=False)

print("Files saved locally ")

#  UPLOAD TO MINIO
def upload_to_minio(df, filename):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    if BUCKET not in [b["Name"] for b in s3.list_buckets().get("Buckets", [])]:
        s3.create_bucket(Bucket=BUCKET)

    s3.put_object(
        Bucket=BUCKET,
        Key=f"gold/{filename}",
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    print(f"Uploaded to MinIO: {filename}")

#  UPLOAD FILES 
upload_to_minio(dim_crypto, "dim_crypto.parquet")
upload_to_minio(dim_date, "dim_date.parquet")
upload_to_minio(fact, "fact_market.parquet")

print("GOLD PIPELINE DONE SUCCESSFULLY ")