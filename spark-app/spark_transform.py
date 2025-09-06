import os
from pyspark.sql import SparkSession
from minio import Minio
import pyarrow.parquet as pq
from dotenv import load_dotenv


# Configuration
load_dotenv()

# MinIO config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_SECURE = os.getenv("MINIO_SECURE") == 'True'

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

spark = SparkSession.builder \
    .appName("OlistETL") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

