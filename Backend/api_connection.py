import requests
import curl

base_url = "https://api.kroger.com/v1/products"
response = requests.get(base_url, params={"filter.limit": 10})
print(response)
print(response.json())