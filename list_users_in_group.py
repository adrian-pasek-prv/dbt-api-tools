from api_url_header import return_api_url_header
import requests

base_url = return_api_url_header()["base_url"]
headers = return_api_url_header()["headers"]

response = requests.get(base_url + "users", headers=headers)

print(base_url + "users")
print(headers)

print(response.json())