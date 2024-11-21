import configparser

parser = configparser.ConfigParser()
parser.read("conf")

DBT_ACCOUNT_ID = parser.get("dbt_credentials", "account_id")
DBT_ACCESS_TOKEN = parser.get("dbt_credentials", "access_token")

def return_api_url_header(api_version="v3", account_id=DBT_ACCOUNT_ID):
    return {
        "base_url": f"https://cloud.getdbt.com/api/{api_version}/accounts/{account_id}/",
        "headers": {
            "Accept": "application/json",
            "Authorization": f"Bearer {DBT_ACCESS_TOKEN}"
        }
    }