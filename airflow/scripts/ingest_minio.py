import os
import pandas as pd
import logging
logger = logging.getLogger(__name__)

from minio import Minio
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET")
MINIO_SECURE = os.getenv("MINIO_SECURE") == 'True'

DATA_DIR = "/data/raw/"

def csv_to_table_name(csv_file: str) -> str:
    """Convert CSV filename to staging table name."""
    name = os.path.splitext(csv_file)[0]  # remove extension
    return f"staging_{name.replace('olist_', '').replace('_dataset', '')}"

def ingest_ecommerce_data():
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

    if not minio_client.bucket_exists(MINIO_RAW_BUCKET):
        minio_client.make_bucket(MINIO_RAW_BUCKET)

    for csv_file in os.listdir(DATA_DIR):
        if csv_file.endswith(".csv"):
            table_name = csv_to_table_name(csv_file)
            file_path = os.path.join(DATA_DIR, csv_file)
            logger.info(f"Ingesting {csv_file} into {table_name}...")
            df = pd.read_csv(file_path)
            parquet_file = f"{table_name}.parquet"
            df.to_parquet(parquet_file, index=False)
            minio_client.fput_object(MINIO_RAW_BUCKET, parquet_file, parquet_file)
            os.remove(parquet_file)
            logger.info(f"Ingested {csv_file} into {table_name}.")
