import base64
import curl
import requests
import dotenv
import os 

def kroger_api_key():
    # Replace with your actual client ID and client secret
    client_id = dotenv.get_key('.env', 'Kroger_Client_ID')
    client_secret = dotenv.get_key('.env', 'Kroger_Client_Secret')
    credentials = f'{client_id}:{client_secret}'
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    url = 'https://api.kroger.com/v1/connect/oauth2/token'
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": f"Basic {encoded_credentials}"
    }
    data = {
        "grant_type": "client_credentials"
    }
    response = requests.post(url, headers=headers, data=data)
    access_token = response.json().get('access_token')
    print(response.text)
    print(response.status_code)
    print(response.json())
    
    print(f'\n\nAccess Token:')
    return access_token

kroger_api_key()