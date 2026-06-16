import json
import pandas as pd
import boto3
import os
from io import BytesIO
from dotenv import load_dotenv
from botocore.config import Config

load_dotenv()

BRONZE_PATH = os.getenv("BRONZE_PATH")
SILVER_PATH = os.getenv("SILVER_PATH")
BUCKET = os.getenv("SILVER_BUCKET")

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    config=Config(signature_version="s3v4")
)

# ---------------- LOAD ----------------
def load_data():
    with open(BRONZE_PATH, "r", encoding="utf-8") as f:
        return pd.DataFrame(json.load(f))

# ---------------- TRANSFORM ----------------
def transform(df):

    df.columns = df.columns.str.lower()

    cols = [
        "id","symbol","name",
        "current_price","market_cap",
        "total_volume","high_24h","low_24h",
        "price_change_percentage_24h",
        "date_extraction"
    ]

    df = df[[c for c in cols if c in df.columns]].copy()

    df["date_extraction"] = pd.to_datetime(df["date_extraction"], errors="coerce")

    numeric_cols = [
        "current_price","market_cap",
        "total_volume","high_24h","low_24h",
        "price_change_percentage_24h"
    ]

    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.drop_duplicates(subset=["id"])

    df["price_range_24h"] = df["high_24h"] - df["low_24h"]
    df["full_date"] = df["date_extraction"].dt.floor("D")

    return df

# ---------------- SAVE 
def save(df):

    os.makedirs(os.path.dirname(SILVER_PATH), exist_ok=True)

    file_path = os.path.join(os.path.dirname(SILVER_PATH), "crypto_silver.parquet")

    df.to_parquet(file_path, index=False)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    try:
        buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        if BUCKET not in buckets:
            s3.create_bucket(Bucket=BUCKET)

        s3.put_object(
            Bucket=BUCKET,
            Key="crypto-silver/crypto_silver.parquet",
            Body=buffer.getvalue(),
            ContentType="application/octet-stream"
        )
    except Exception as e:
        print("MinIO not available:", e)

    print("✔ Silver created:", file_path)

if __name__ == "__main__":
    df = load_data()
    df = transform(df)
    save(df)