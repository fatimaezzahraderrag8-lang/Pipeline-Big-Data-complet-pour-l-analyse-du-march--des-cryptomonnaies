import json
import requests
import boto3
import os
import sys
import logging
from datetime import datetime, UTC
from pathlib import Path
from dotenv import load_dotenv
from botocore.config import Config

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Charger .env
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

URL = os.getenv("COINGECKO_URL")
PATH = os.getenv("BRONZE_PATH")
BUCKET = os.getenv("BRONZE_BUCKET")

logging.info(f" Fichier .env = {env_path}")
logging.info(f" URL = {URL}")
logging.info(f" PATH = {PATH}")
logging.info(f" BUCKET = {BUCKET}")

# Client MinIO
try:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        config=Config(signature_version="s3v4")
    )
except Exception as e:
    logging.error("❌ Connexion à MinIO échouée : %s", e)
    sys.exit(1)


def fetch():
    if not URL:
        raise ValueError(" COINGECKO_URL n'est pas défini dans .env")

    try:
        r = requests.get(
            URL,
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 10,
                "page": 1,
                "sparkline": "false"
            },
            timeout=30
        )
        r.raise_for_status()
        data = r.json()

        for item in data:
            item["date_extraction"] = datetime.now(UTC).isoformat()

        logging.info(" Données récupérées depuis CoinGecko")
        return data

    except Exception as e:
        logging.error(" Erreur lors de la récupération des données CoinGecko : %s", e)
        raise


def save_bronze(data):
    if not PATH:
        raise ValueError(" BRONZE_PATH n'est pas défini dans .env")

    try:
        directory = os.path.dirname(PATH)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with open(PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        logging.info(" Données Bronze sauvegardées en local")

        if not BUCKET:
            raise ValueError(" BRONZE_BUCKET n'est pas défini dans .env")

        # Vérifier si le bucket existe
        buckets = s3.list_buckets().get("Buckets", [])
        existing_buckets = [b["Name"] for b in buckets]

        if BUCKET not in existing_buckets:
            s3.create_bucket(Bucket=BUCKET)
            logging.info("✔ Bucket créé : %s", BUCKET)

        key = f"crypto-bronze/{datetime.now(UTC).strftime('%Y/%m/%d/%H%M%S')}/raw.json"

        with open(PATH, "rb") as f:
            s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=f,
                ContentType="application/json"
            )

        logging.info("✔ Données Bronze envoyées vers MinIO")

    except Exception as e:
        logging.error(" Erreur lors de la sauvegarde Bronze : %s", e)
        raise


if __name__ == "__main__":
    try:
        data = fetch()
        save_bronze(data)
    except Exception as e:
        logging.error(" Échec de ingest_bronze : %s", e)
        sys.exit(1)
