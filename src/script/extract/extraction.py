import io
import json
import os

import requests
from minio import Minio


def get_minio_client() -> Minio:
    return Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "12345678"),
        secure=False,
    )


def _upload_to_bronze(client: Minio, data: list, filename: str) -> None:
    bucket = "data-lake"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    data_bytes = json.dumps(data).encode("utf-8")
    client.put_object(
        bucket,
        f"bronze/{filename}",
        io.BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/json",
    )
    print(f"[extract] {filename} enviado para bronze!")


def extract_products(**kwargs) -> None:
    client = get_minio_client()
    response = requests.get("https://fakestoreapi.com/products", timeout=30)
    response.raise_for_status()
    _upload_to_bronze(client, response.json(), "products.json")


def extract_users(**kwargs) -> None:
    client = get_minio_client()
    response = requests.get("https://fakestoreapi.com/users", timeout=30)
    response.raise_for_status()
    _upload_to_bronze(client, response.json(), "users.json")


def extract_carts(**kwargs) -> None:
    client = get_minio_client()
    response = requests.get("https://fakestoreapi.com/carts", timeout=30)
    response.raise_for_status()
    _upload_to_bronze(client, response.json(), "carts.json")
