import configparser

parser = configparser.ConfigParser()
parser.read("config")

DBT_ACCOUNT_ID = parser.get("dbt_credentials", "account_id")
DBT_ACCESS_TOKEN = parser.get("dbt_credentials", "access_token")
GLOBAL_DATA_MODEL_PROJECT_ID = 4647

def return_api_url_header(api_version="v2", account_id=DBT_ACCOUNT_ID):
    """Returns the base URL and headers for the DBT API."""
    return {
        "base_url": f"https://emea.dbt.com/api/{api_version}/accounts/{account_id}/",
        "headers": {
            "Accept": "application/json",
            "Authorization": f"Bearer {DBT_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }
    }