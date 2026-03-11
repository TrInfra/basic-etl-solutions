import io
import json
import os

from dotenv import load_dotenv
import pandas as pd
from minio import Minio

load_dotenv()


def get_minio_client() -> Minio:
    return Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )


def _read_bronze(client: Minio, filename: str) -> list:
    response = client.get_object("data-lake", f"bronze/{filename}")
    return json.loads(response.read())


def _upload_to_silver(client: Minio, df: pd.DataFrame, filename: str) -> None:
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    client.put_object(
        "data-lake",
        f"silver/{filename}",
        csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="application/csv",
    )
    print(f"[transform] {filename} enviado para silver!")


def transform_products(**kwargs) -> None:
    client = get_minio_client()
    df = pd.DataFrame(_read_bronze(client, "products.json"))

    df["rating_rate"] = df["rating"].apply(lambda x: x["rate"])
    df["rating_count"] = df["rating"].apply(lambda x: x["count"])
    df = df.drop(columns=["image", "rating"])
    df["category"] = df["category"].str.replace("'", "").str.replace(" ", "_")

    _upload_to_silver(client, df, "products_silver.csv")


def transform_users(**kwargs) -> None:
    client = get_minio_client()
    df = pd.DataFrame(_read_bronze(client, "users.json"))

    df["first_name"] = df["name"].apply(lambda x: x["firstname"])
    df["last_name"] = df["name"].apply(lambda x: x["lastname"])
    df["city"] = df["address"].apply(lambda x: x["city"])
    df["street"] = df["address"].apply(lambda x: x["street"])
    df["number"] = df["address"].apply(lambda x: x["number"])
    df["zipcode"] = df["address"].apply(lambda x: x["zipcode"])
    df = df.drop(columns=["name", "address", "password", "__v"])

    _upload_to_silver(client, df, "users_silver.csv")


def transform_carts(**kwargs) -> None:
    client = get_minio_client()
    df = pd.DataFrame(_read_bronze(client, "carts.json"))

    df = df.explode("products")
    df["productId"] = df["products"].apply(lambda x: x["productId"])
    df["quantity"] = df["products"].apply(lambda x: x["quantity"])
    df = df.drop(columns=["products", "__v"])

    _upload_to_silver(client, df, "carts_silver.csv")


