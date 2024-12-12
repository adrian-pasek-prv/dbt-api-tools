from api_url_header import return_api_url_header, GLOBAL_DATA_MODEL_PROJECT_ID
import requests
from datetime import datetime, timezone

base_url = return_api_url_header(api_version="v3")["base_url"]
headers = return_api_url_header(api_version="v3")["headers"]

response = requests.get(
    base_url 
    + f"projects/{GLOBAL_DATA_MODEL_PROJECT_ID}/credentials", headers=headers
    ).json()

update_credentials = []
for credential in response["data"]:
    if credential["user"] == "ADRIAN.PASEK@PAYU.COM" and credential["state"] == 1:
        update_credentials.append(
            {   "credential_id": credential["id"],
                "account_id": credential["account_id"],
                "project_id": credential["project_id"],
                "auth_type": credential["auth_type"],
                "type": credential["type"],
                "state": credential["state"],
                "threads": 16,
                "username": credential["user"],
                "schema": credential["schema"],
                "target_name": "dev",
                "warehouse": "BI_DATA_ENGINEER_LIGHT_WH",
                "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f%z")
            }
        )
    continue

print(update_credentials)
print(1)