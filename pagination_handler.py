import requests
from typing import Generator, Dict, Optional, Any
import time

def paginate_response(endpoint: str, 
                      headers: Dict[str, Any], 
                      params: Optional[Dict[str, Any]] = None, 
                      chunk_size: int = 100,
                      max_retries: int = 3,
                      timeout: int = 10
) -> Generator:
    """
    Paginate dbt Cloud API responses and yield batches of records.

    Args:
        endpoint (str): URL of the dbt Cloud API endpoint
        headers (Dict[str, str]): HTTP headers for authentication/authorization
        params (Optional[Dict[str, Any]]): Additional query parameters (excluding offset/limit). Defaults to None.
        chunk_size (int, optional): Number of records per batch. Defaults to 100.
        max_retries (int, optional): Maximum retries for transient errors. Defaults to 3.
        timeout (int, optional): Timeout in seconds for API requests. Defaults to 10.

    Yields:
        list: Batch of records from the API response

    Raises:
        ValueError: If params include offset or limit
        Exception: For API errors or connection issues
    """

    # Initialize pagination variables
    offset = 0
    limit = chunk_size
    retries = 0

    # Check if params include offset and limit, if not then expand the copied dict
    params = params if params else {}
    if "offset" in params or "limit" in params:
        raise ValueError("params must not include offset or limit")
    params.update({"offset": offset, "limit": limit})
    
    while True:
        try:
            time.sleep(1)
            response = requests.get(endpoint,
                                    headers=headers,
                                    params=params,
                                    timeout=timeout)
            response.raise_for_status()
            response = response.json()
            records = response["data"]
            total_count = response["extra"]["pagination"]["total_count"]

            # Yield current batch of data
            yield records

            records_count = len(records)
            offset += records_count
            params["offset"] = offset

            if offset >= total_count:
                print(f"Pagination complete. Returned {offset} records")
                break

        except requests.exceptions.HTTPError as e:
            if retries < max_retries:
                retries += 1
                continue
            raise e
        except Exception as e:
            raise e

