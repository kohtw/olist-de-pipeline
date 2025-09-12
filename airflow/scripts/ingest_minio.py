import os
import pandas as pd
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_RAW_BUCKET")
MINIO_SECURE = os.getenv("MINIO_SECURE") == 'True'

DATA_DIR = "/data/raw/"

csv_table_map = {
    "olist_customers_dataset.csv": "staging_customers",
    "olist_orders_dataset.csv": "staging_orders",
    "olist_order_items_dataset.csv": "staging_order_items",
    "olist_products_dataset.csv": "staging_products",
    "olist_sellers_dataset.csv": "staging_sellers",
    "olist_order_payments_dataset.csv": "staging_payments",
    "olist_order_reviews_dataset.csv": "staging_reviews",
    "olist_geolocation_dataset.csv": "staging_geolocation",
    "product_category_name_translation.csv": "staging_product_categories"
}

def ingest_data():
    # Debug: Check which variables are None
    print(f"Endpoint: {MINIO_ENDPOINT} (type: {type(MINIO_ENDPOINT)})")
    print(f"Access Key: {MINIO_ACCESS_KEY} (type: {type(MINIO_ACCESS_KEY)})")
    print(f"Secret Key: {MINIO_SECRET_KEY} (type: {type(MINIO_SECRET_KEY)})")
    print(f"Bucket: {MINIO_BUCKET} (type: {type(MINIO_BUCKET)})")
    print(f"Secure: {MINIO_SECURE} (type: {type(MINIO_SECURE)})")
    
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)

    for csv_file, table_name in csv_table_map.items():
        file_path = os.path.join(DATA_DIR, csv_file)
        if os.path.exists(file_path):
            print(f"[INFO] Ingesting {csv_file} into {table_name}...")
            df = pd.read_csv(file_path)
            parquet_file = f"{table_name}.parquet"
            df.to_parquet(parquet_file, index=False)
            minio_client.fput_object(MINIO_BUCKET, parquet_file, parquet_file)
            os.remove(parquet_file)
            print(f"[SUCCESS] Ingested {csv_file} into {table_name}.")
        else:
            print(f"[WARNING] File {file_path} does not exist. Skipping...")
