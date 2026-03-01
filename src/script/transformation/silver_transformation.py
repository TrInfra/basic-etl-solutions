import pandas as pd
import json
from minio import Minio
import io
import os
from dotenv import load_dotenv

load_dotenv()

client = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)
# Lendo o arquivo bronze
response = client.get_object("data-lake", "bronze/products.json")
products = json.loads(response.read())

# Transformando em DataFrame com pandas PRODUTOS
df = pd.DataFrame(products)

print(df.head())
print(df.columns)

# Separando o rating em duas colunas
df['rating_rate'] = df['rating'].apply(lambda x: x['rate'])
df['rating_count'] = df['rating'].apply(lambda x: x['count'])

# Removendo colunas desnecessárias
df = df.drop(columns=['image', 'rating'])

df['category'] = df['category'].str.replace("'", "").str.replace(" ", "_")

print(df.head())
print(df.columns)

# Salvando o arquivo silver
csv_buffer = io.BytesIO()
df.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)
client.put_object(
    "data-lake",
    "silver/products_silver.csv",
    csv_buffer,
    length=csv_buffer.getbuffer().nbytes,
    content_type="application/csv"
)
print("Products silver enviado para o MinIO!")

# Lendo o arquivo bronze de USERS
response = client.get_object("data-lake", "bronze/users.json")
users = json.loads(response.read())

# Transformando em DataFrame USERS
df = pd.DataFrame(users)
print(df.head()) 
print(df.columns)

df['first_name'] = df['name'].apply(lambda x: x['firstname']) 
df['last_name'] = df['name'].apply(lambda x: x['lastname'])

df['city'] = df['address'].apply(lambda x: x['city'])
df['street'] = df['address'].apply(lambda x: x['street'])
df['number'] = df['address'].apply(lambda x: x['number'])
df['zipcode'] = df['address'].apply(lambda x: x['zipcode'])

# Removendo colunas desnecessárias
df = df.drop(columns=['name', 'address', 'password', '__v'])

# Salvando
csv_buffer = io.BytesIO()
df.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)
client.put_object(
    "data-lake",
    "silver/users_silver.csv",
    csv_buffer,
    length=csv_buffer.getbuffer().nbytes,
    content_type="application/csv"
)
print("Users silver enviado para o MinIO!")



#-------------------------------------------------------------
# Lendo o arquivo bronze
response = client.get_object("data-lake", "bronze/carts.json")
carts = json.loads(response.read())


df = pd.DataFrame(carts)

# Explodindo a lista de products
df = df .explode('products')

# Extraindo productId e quantity
df['productId'] = df['products'].apply(lambda x: x['productId'])
df['quantity'] = df['products'].apply(lambda x: x['quantity'])

# Removendo colunas desnecessárias
df = df.drop(columns=['products', '__v'])

print(df.head())
print(df.columns)

# Salvando o arquivo silver
csv_buffer = io.BytesIO()
df.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)
client.put_object(
    "data-lake",
    "silver/carts_silver.csv",
    csv_buffer,
    length=csv_buffer.getbuffer().nbytes,
    content_type="application/csv"
)
print("Carts silver enviado para o MinIO!")


