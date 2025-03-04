from api_url_header import return_api_url_header, GLOBAL_DATA_MODEL_PROJECT_ID
import requests

def duplicate_dbt_job(job_id: int, new_job_name: str, new_description: str, new_cron: str, project_id: int, api_ver: str = "v2") -> None:
    
    base_url = return_api_url_header(api_version=api_ver)["base_url"]
    headers = return_api_url_header(api_version=api_ver)["headers"]

    response = requests.get(
        base_url
        + f"jobs/{job_id}",
        headers=headers
    ).json()

    if response["status"]["code"] == 200:
        response_data = response["data"]
        data = {
            "account_id": response_data["account_id"],
            "project_id": response_data["project_id"],
            "environment_id": response_data["environment_id"],
            "name": new_job_name,
            "dbt_version": response_data["dbt_version"],
            "description": new_description,
            "execute_steps": response_data["execute_steps"],
            "execution": response_data["execution"],
            "generate_docs": response_data["generate_docs"],
            "job_type": response_data["job_type"],
            "settings": response_data["settings"],
            "state": response_data["state"],
            "generate_sources": response_data["generate_sources"],
            "schedule": {'date': {'type': 'custom_cron', 'cron': new_cron}, 'time': {'type': 'every_hour', 'interval': 1}, 'cron': new_cron}
        }

        # Create the new job
        new_job_response = requests.post(
            base_url
            + f"jobs",
            headers=headers,
            json=data
        ).json()
        if new_job_response["status"]["is_success"] == True:
            print("Job duplicated successfully!")
        else:
            print(f'Failed to duplicate job. Status code: {new_job_response["status"]["code"]}')
    else:
        print(f'Failed to duplicate job. Status code: {response.status_code}')


duplicate_dbt_job(job_id=287804, 
                  new_job_name="Approval Rate PayU Poland (Weekly) - adrian.pasek@payu.com",
                  new_description="Weekly job to refresh parent/upstream models to mart_payupoland__approval_rate and mart_payupoland__approval_rate_hour",
                  new_cron="30 7 * * 7",
                  project_id=GLOBAL_DATA_MODEL_PROJECT_ID,
                  api_ver="v2")
