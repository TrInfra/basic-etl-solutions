import requests as r
import json
import io
import os
from minio import Minio
from dotenv import load_dotenv

# Conectando no MinIO
load_dotenv()

client = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)
def upload_to_minio(data, filename):
    data_bytes = json.dumps(data).encode('utf-8')
    client.put_object(
        "data-lake",
        f"bronze/{filename}",
        io.BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/json"
    )
    print(f"{filename} enviado para o bronze!")

#produtos para sampledata/products.json
response = r.get('https://fakestoreapi.com/products')
products = response.json()
upload_to_minio(products, "products.json")


# Usuários para sampledata/usuarios.json
response = r.get('https://fakestoreapi.com/users')
users = response.json()
upload_to_minio(users, "users.json")

# Carrinhos para sampledata/carrinho.json
response = r.get('https://fakestoreapi.com/carts')
carts = response.json()
upload_to_minio(carts, "carts.json")
