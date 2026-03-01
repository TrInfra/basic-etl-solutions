import pandas as pd
import json

# Lendo o arquivo bronze
with open("../extract/sampledata/products.json", "r") as f:
    products = json.load(f)

# Transformando em DataFrame PRODUTOS
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
df.to_csv("../extract/sampledata/products_silver.csv", index=False)
print("Products silver salvo!")

# Lendo o arquivo bronze de USERS
with open("../extract/sampledata/users.json", "r") as f:
    users = json.load(f)

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
df.to_csv("../extract/sampledata/users_silver.csv", index=False)
print("Users silver salvo!")



#-------------------------------------------------------------
# Lendo o arquivo bronze
with open("../extract/sampledata/carts.json", "r") as f:
    carts = json.load(f)


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
df.to_csv("../extract/sampledata/carts_silver.csv", index=False)
print("Products silver salvo!")


