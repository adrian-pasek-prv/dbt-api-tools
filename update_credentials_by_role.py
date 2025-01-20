from api_url_header import return_api_url_header, GLOBAL_DATA_MODEL_PROJECT_ID
import requests
from datetime import datetime, timezone

def update_credentials_by_role(role: str=None, credentials: dict=None, project_id: int=None, api_ver: str ="v3") -> None:

    base_url = return_api_url_header(api_version=api_ver)["base_url"]
    headers = return_api_url_header(api_version=api_ver)["headers"]
    
    credentials_response = requests.get(
        base_url 
        + f"projects/{project_id}/credentials", headers=headers
        ).json()

    for credential in credentials_response["data"]:
        if credential["state"] == 1 and credential["role"] == role:
            patch = requests.patch(
                base_url + f"projects/{project_id}/credentials/{credential['id']}",
                headers=headers,
                json=credentials,
                ).json()
            # Check the response status
            if patch["status"]["code"] == 200:
                print("Update successful!")
            else:
                print(f'Failed to update. Status code: {patch["status"]["code"]}')
                print("User:", patch["data"]["user"])

update_credentials_by_role(role="GLOBAL_ANALYTICS_ENGINEER", 
                           credentials={
                               "threads": 16,
                               "warehouse": "{{env_var('DBT_DEV_WAREHOUSE')}}"
                           },
                           project_id=GLOBAL_DATA_MODEL_PROJECT_ID,
                           api_ver="v3"
)