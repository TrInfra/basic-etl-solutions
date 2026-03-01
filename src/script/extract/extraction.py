import requests as r
import pandas as pd
import json
import os

#produtos para sampledata/products.json
response = r.get('https://fakestoreapi.com/products')
products = response.json()
with open("sampledata/products.json", "w") as f:
    json.dump(products, f, indent=4)
print("Produtos salvos!")


# Usuários para sampledata/usuarios.json
response = r.get('https://fakestoreapi.com/users')
users = response.json()
with open("sampledata/users.json", "w") as f:
    json.dump(users, f, indent=4)
print("Usuários salvos!")

# Carrinhos para sampledata/carrinho.json
response = r.get('https://fakestoreapi.com/carts')
carts = response.json()
with open("sampledata/carts.json", "w") as f:
    json.dump(carts, f, indent=4)
print("Carrinhos salvos!")

