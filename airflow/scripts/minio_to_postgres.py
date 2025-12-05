import os
import tempfile
import pandas as pd
import logging

from minio import Minio
from sqlalchemy import create_engine
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_PROCESSED_BUCKET = os.getenv("MINIO_PROCESSED_BUCKET")
MINIO_SECURE = os.getenv("MINIO_SECURE") == 'True'

POSTGRES_CONN = os.getenv("POSTGRES_CONN")
engine = create_engine(POSTGRES_CONN)

def object_to_table_name(obj_name: str) -> str:
    """Convert object filename to analytics table name."""
    base = os.path.splitext(obj_name)[0]  # remove .parquet/.csv
    return f"analytics.{base}"

def load_ecommerce_data():    
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

    if not minio_client.bucket_exists(MINIO_PROCESSED_BUCKET):
            raise RuntimeError(f"Bucket {MINIO_PROCESSED_BUCKET} does not exist")

    for item in minio_client.list_objects(MINIO_PROCESSED_BUCKET, recursive=True):
            obj_name = item.object_name
            table_name = object_to_table_name(obj_name)

            logger.info(f"Loading {obj_name} → {table_name}")   

            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                minio_client.fget_object(MINIO_PROCESSED_BUCKET, obj_name, tmp.name)

                # autodetect format
                if obj_name.endswith(".parquet"):
                    df = pd.read_parquet(tmp.name)
                else:
                    logger.warning(f"Skipping unsupported file: {obj_name}")
                    continue

            # write into Postgres
            df.to_sql(
                name=table_name.split(".")[1],   # table
                con=engine,
                schema="analytics",
                if_exists="replace",
                index=False
            )

            logger.info(f"Loaded {obj_name} → {table_name}")