from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.S3_hook import S3Hook
import os

BUCKET = os.getenv("MINIO_BUCKET", "truck-telemetry")

with DAG(
    dag_id="train_from_minio",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ml", "minio"],
):

    @task
    def fetch_latest_partition(ds=None):
        hook = S3Hook(aws_conn_id="minio_s3")
        # Example: list keys for the run date partition; adjust prefix as needed
        keys = hook.list_keys(bucket_name=BUCKET, prefix=f"TRUCK-001/{ds}/") or []
        return keys

    @task
    def train_model(keys: list[str]):
        # Stub: replace with your ML training logic
        print(f"Training on {len(keys)} objects: {keys[:5]}")

    train_model(fetch_latest_partition())


