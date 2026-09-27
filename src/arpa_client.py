"""
ARPA Lombardia SODA API Client with throttling management and pagination.
Supports fetching monitoring stations (ib47-atvt) and sensor measurements (nicp-bhqi).
"""

import json
import logging
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Generator, List, Optional

logger = logging.getLogger(__name__)

# Socrata Open Data endpoints
BASE_URL = "https://www.dati.lombardia.it/resource"
DATASET_STATIONS = "ib47-atvt"
DATASET_MEASUREMENTS = "nicp-bhqi"


class ArpaSocrataClient:
    """
    Client for querying ARPA Lombardia Open Data (SODA API) with built-in
    throttling protection, exponential backoff, and pagination.
    """

    def __init__(
        self,
        base_url: str = BASE_URL,
        app_token: Optional[str] = None,
        max_retries: int = 5,
        base_delay: float = 2.0,
        polite_delay: float = 0.5,
        timeout: int = 60,
    ):
        """
        :param base_url: SODA base URL.
        :param app_token: Optional Socrata App Token (X-App-Token header).
        :param max_retries: Max retry attempts when encountering rate limits or temporary server errors.
        :param base_delay: Initial backoff delay in seconds.
        :param polite_delay: Delay between successful consecutive requests to prevent rate limiting.
        :param timeout: HTTP request timeout in seconds.
        """
        self.base_url = base_url.rstrip("/")
        self.app_token = app_token
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.polite_delay = polite_delay
        self.timeout = timeout

    def _execute_request(self, url: str) -> List[Dict[str, Any]]:
        """
        Executes an HTTP GET request with retry and exponential backoff on HTTP 429
        (Too Many Requests) or 5xx server errors.
        """
        headers = {
            "Accept": "application/json",
            "User-Agent": "ARPA-Bergamo-Monitoring/1.0",
        }
        if self.app_token:
            headers["X-App-Token"] = self.app_token

        req = urllib.request.Request(url, headers=headers)

        retries = 0
        while True:
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    raw_data = response.read().decode("utf-8")
                    data = json.loads(raw_data)
                    return data if isinstance(data, list) else []

            except urllib.error.HTTPError as err:
                is_throttled = err.code == 429
                is_server_error = 500 <= err.code < 600

                if (is_throttled or is_server_error) and retries < self.max_retries:
                    retries += 1
                    # Exponential backoff with random jitter to avoid thundering herd
                    backoff = self.base_delay * (2 ** (retries - 1)) + random.uniform(0.1, 1.0)
                    retry_reason = "Rate limited (HTTP 429)" if is_throttled else f"Server error ({err.code})"
                    logger.warning(
                        "%s on %s. Retrying attempt %d/%d in %.2fs...",
                        retry_reason,
                        url,
                        retries,
                        self.max_retries,
                        backoff,
                    )
                    time.sleep(backoff)
                else:
                    logger.error("HTTP error %d fetching %s: %s", err.code, url, err.reason)
                    raise

            except urllib.error.URLError as err:
                if retries < self.max_retries:
                    retries += 1
                    backoff = self.base_delay * (2 ** (retries - 1)) + random.uniform(0.1, 1.0)
                    logger.warning(
                        "Network error (%s). Retrying attempt %d/%d in %.2fs...",
                        err.reason,
                        retries,
                        self.max_retries,
                        backoff,
                    )
                    time.sleep(backoff)
                else:
                    logger.error("Network error fetching %s: %s", url, err.reason)
                    raise

    def get_stations(
        self,
        province: str = "BG",
        dataset_id: str = DATASET_STATIONS,
        limit: int = 50000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch stations and sensors metadata.
        :param province: Province filter (e.g. 'BG' for Bergamo). Pass None or 'ALL' for entire region.
        :param dataset_id: Socrata dataset ID for stations.
        :param limit: Max rows to return.
        """
        query_params = {
            "$limit": str(limit),
            "$order": "idsensore ASC",
        }
        if province and province.upper() != "ALL":
            query_params["$where"] = f"provincia = '{province.upper()}'"

        query_string = urllib.parse.urlencode(query_params)
        url = f"{self.base_url}/{dataset_id}.json?{query_string}"
        logger.info("Fetching stations from: %s", url)
        return self._execute_request(url)

    def get_measurements_page(
        self,
        dataset_id: str = DATASET_MEASUREMENTS,
        where_clause: Optional[str] = None,
        limit: int = 50000,
        offset: int = 0,
        order_by: str = "data DESC",
    ) -> List[Dict[str, Any]]:
        """
        Fetch a single page of sensor measurements.
        """
        query_params = {
            "$limit": str(limit),
            "$offset": str(offset),
            "$order": order_by,
        }
        if where_clause:
            query_params["$where"] = where_clause

        query_string = urllib.parse.urlencode(query_params)
        url = f"{self.base_url}/{dataset_id}.json?{query_string}"
        return self._execute_request(url)

    def iter_measurements_for_sensors(
        self,
        sensor_ids: List[int | str],
        start_date: Optional[str] = None,
        chunk_size_sensors: int = 40,
        page_size: int = 50000,
        max_records_per_chunk: Optional[int] = None,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Generator yielding batches of measurements for a given list of sensor IDs.
        Chunks sensor IDs to prevent URI-length limitations and paginates through records.
        Yields list of records per page.
        """
        if not sensor_ids:
            return

        sensor_id_strs = [str(sid) for sid in sensor_ids]

        # Chunk sensors in groups (e.g. 40 sensors) to keep SoQL IN(...) within URL safe length
        for i in range(0, len(sensor_id_strs), chunk_size_sensors):
            sensor_chunk = sensor_id_strs[i : i + chunk_size_sensors]
            in_clause = ",".join(f"'{sid}'" for sid in sensor_chunk)
            where_conditions = [f"idsensore in({in_clause})"]

            if start_date:
                where_conditions.append(f"data >= '{start_date}'")

            where_clause = " AND ".join(where_conditions)
            offset = 0
            records_fetched_for_chunk = 0

            while True:
                logger.info(
                    "Fetching measurements for sensor chunk %d-%d (offset: %d, limit: %d)",
                    i,
                    min(i + chunk_size_sensors, len(sensor_id_strs)),
                    offset,
                    page_size,
                )
                page = self.get_measurements_page(
                    where_clause=where_clause,
                    limit=page_size,
                    offset=offset,
                    order_by="data DESC",
                )

                if not page:
                    break

                yield page

                records_fetched_for_chunk += len(page)
                if len(page) < page_size:
                    # Reached end of records for this chunk
                    break

                if max_records_per_chunk and records_fetched_for_chunk >= max_records_per_chunk:
                    break

                offset += page_size

                # Polite delay between consecutive pages to avoid throttling
                if self.polite_delay > 0:
                    time.sleep(self.polite_delay)
