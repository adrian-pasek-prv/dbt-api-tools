from api_url_header import return_api_url_header
from pagination_handler import paginate_response
import csv
import argparse
import requests
from copy import deepcopy

def offset_cron_hour(cron: str, offset: int) -> str:
    cron_split = cron.split()

    if len(cron_split) != 5:
        raise ValueError("Invalid CRON format")

    hour_field = cron_split[1]

    adjusted_hours = []
    for part in hour_field.split(","):
        if "-" in part:
            start, end = part.split("-")
            start_offset, end_offset = map(lambda x: (int(x) + offset) % 24, (start, end))
            adjusted_hours.append(f"{start_offset}-{end_offset}")
        elif part == "*" or "*/" in part:
            adjusted_hours.append(part)
        elif "/" in part:
            single_hour_offset = (int(part.split("/")[0]) + offset) % 24
            adjusted_hours.append(str(single_hour_offset) + "/" + part.split("/")[1])
        else:
            single_hour_offset = (int(part) + offset) % 24
            adjusted_hours.append(str(single_hour_offset))
        
    cron_split[1] = ",".join(adjusted_hours) if len(adjusted_hours) > 1 else adjusted_hours[0]
    
    return " ".join(cron_split)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Offset cron schedule in dbt Cloud jobs")
    parser.add_argument("--project_id", required=True, type=int, help="dbt Project ID")
    parser.add_argument("--offset", required=True, type=int, help="Offset in hours")
    parser.add_argument("--exclude_keywords", required=False, nargs="+",
                        help="List of keywords to exclude in search for dbt Job name. Seperated by space, only uppercase")
    parser.add_argument("--send_post_request", action="store_true", help="Send POST request to update job")
    args = parser.parse_args()

    project_id = args.project_id
    offset = args.offset 
    exclude_keywords = args.exclude_keywords
    send_post_request = args.send_post_request

    api_ver = "v2"
    base_url = return_api_url_header(api_version=api_ver)["base_url"]
    headers = return_api_url_header(api_version=api_ver)["headers"]

    results = paginate_response(
        endpoint=base_url + f"jobs",
        headers=headers,
        chunk_size=100,
        max_retries=3,
        timeout=10,
        params={"project_id": project_id}
    )

    # Iterate over a generator
    records = []
    for batch in results:
        records.extend(batch)

    # Iterate over records, exclude keywords, and write results to csv file
    with open("jobs_with_adjusted_cron.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["job_name", "original_cron", "adjusted_cron"])
        for job in records:
            if job["job_type"] == "scheduled" \
                and job["state"] == 1 \
                and job["schedule"]["cron"] \
                and not any(keyword in job["name"] for keyword in exclude_keywords):
                    # Write to CSV a job_name, original_cron, adjusted_cron
                    adjusted_cron = offset_cron_hour(job["schedule"]["cron"], offset)
                    writer.writerow([job["name"], job["schedule"]["cron"], adjusted_cron])
                    # Send POST request only if send_post_request is True
                    if send_post_request and job["name"] == "After Merge - adrian.pasek@payu.com":
                       include_keys = ["account_id", "project_id", "environment_id", "name",
                                       "dbt_version", "deferring_environment_id", "deferring_job_definition_id",
                                       "description", "execute_steps", "execution", "generate_docs",
                                       "job_type", "lifecycle_webhooks", "run_compare_changes", "compare_changes_flags",
                                       "run_generate_sources", "run_lint", "errors_on_lint_failure", "settings", 
                                       "state", "triggers_on_draft_pr", "triggers", "schedule", "generate_sources",
                                       ]
                       updated_job = {key: deepcopy(job[key]) for key in include_keys}
                       updated_job["schedule"]["date"]["type"] = "custom_cron"
                       updated_job["schedule"]["date"]["cron"] = adjusted_cron
                       updated_job["schedule"]["cron"] = adjusted_cron
                       response = requests.post(
                           url=base_url + f"jobs/{job['id']}",
                           headers=headers,
                           json=updated_job
                       )
                       if response.status_code == 200:
                           print(f"Job {job['name']} updated successfully. With cron: '{adjusted_cron}'")

    
    







