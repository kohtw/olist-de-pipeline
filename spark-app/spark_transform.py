import os
from minio import Minio
from pyspark.sql import SparkSession
from dotenv import load_dotenv

def transform_data():
    load_dotenv()

    SPARKMASTER = os.getenv("SPARK_MASTER")

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")    
    MINIO_SECURE = os.getenv("MINIO_SECURE") == 'True'
    MINIO_RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET")
    MINIO_PROCESSED_BUCKET = os.getenv("MINIO_PROCESSED_BUCKET")

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

    if not minio_client.bucket_exists(MINIO_PROCESSED_BUCKET):
        minio_client.make_bucket(MINIO_PROCESSED_BUCKET)

    spark = SparkSession.builder \
        .appName("OlistETL") \
        .master(SPARKMASTER) \
        .getOrCreate()

    # sample steps, to be changed
    df_orders = spark.read.parquet(f"s3a://{MINIO_RAW_BUCKET}/staging_orders")
    df_orders_transformed = df_orders.dropDuplicates(["order_id"])
    df_orders_transformed.write.mode("overwrite").parquet(f"s3a://{MINIO_PROCESSED_BUCKET}/orders")

    spark.stop()
