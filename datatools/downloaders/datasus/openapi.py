import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import json5
import requests

from .types import MAX_RETRIES, PRE_URL_API, RETRY_DELAY_SECONDS

logger = logging.getLogger("downloader:DATASUS:OpenAPI")

SWAGGER_URL = f"{PRE_URL_API}/static/swagger.json"
SAVE_INTERVAL = 10
REQUEST_TIMEOUT = 60


def load_swagger_spec() -> Dict[str, Any]:
    logger.info(f"Loading API specification from {SWAGGER_URL}...")
    response = requests.get(SWAGGER_URL)
    response.raise_for_status()
    return json5.loads(response.text)


def extract_endpoints(swagger_spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    endpoints = []
    paths = swagger_spec.get("paths", {})

    for path, methods in paths.items():
        if "{" in path:
            continue

        if "get" not in methods:
            continue

        method_spec = methods["get"]

        if "security" in method_spec:
            continue

        parameters = method_spec.get("parameters", [])
        has_limit = any(p.get("name") == "limit" for p in parameters)
        has_offset = any(p.get("name") == "offset" for p in parameters)

        if has_limit and has_offset:
            max_limit = 100

            endpoint_info = {
                "path": path,
                "summary": method_spec.get("summary", ""),
                "max_limit": max_limit,
                "tag": method_spec.get("tags", ["Unknown"])[0],
            }
            endpoints.append(endpoint_info)

    return endpoints


def sanitize_filename(path: str) -> str:
    filename = path.lstrip("/").replace("/", "_")
    return f"{filename}.json"


def fetch_page(endpoint: str, offset: int, limit: int) -> Optional[Dict[str, Any]]:
    url = f"{PRE_URL_API}{endpoint}"
    params = {"offset": offset, "limit": limit}

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error on attempt {attempt + 1}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logger.error(f"Failed after {MAX_RETRIES} attempts")
                return None


def save_data(filepath: str, data: List[Any], metadata: Dict[str, Any]):
    output = {"metadata": metadata, "data": data}

    with open(filepath, "w", encoding="utf-8") as f:
        json5.dump(output, f, ensure_ascii=False, indent=2)


def download_endpoint(endpoint_info: Dict[str, Any], output_dir: str) -> bool:
    path = endpoint_info["path"]
    max_limit = endpoint_info["max_limit"]

    logger.info("=" * 80)
    logger.info(f"Downloading: {path}")
    logger.info(f"   {endpoint_info['summary']}")
    logger.info(f"   Page limit: {max_limit}")
    logger.info("=" * 80)

    filename = sanitize_filename(path)
    filepath = os.path.join(output_dir, filename)

    all_data = []
    offset = 0
    page_count = 0
    total_records = 0

    start_time = time.time()

    while True:
        page_count += 1
        logger.info(f"Page {page_count} (offset={offset})...")

        result = fetch_page(path, offset, max_limit)

        if result is None:
            logger.error("Download error, stopping this endpoint")
            break

        page_data = []
        if isinstance(result, list):
            page_data = result
        elif isinstance(result, dict):
            keys = list(result.keys())
            key = keys[0]
            page_data = result[key]
        else:
            page_data = [result] if result else []

        records_in_page = len(page_data)
        logger.info(f"{records_in_page} records")

        if records_in_page == 0:
            logger.info("No records returned, finishing endpoint")
            break

        all_data.extend(page_data)
        total_records += records_in_page

        if page_count % SAVE_INTERVAL == 0:
            elapsed = time.time() - start_time
            metadata = {
                "endpoint": path,
                "summary": endpoint_info["summary"],
                "tag": endpoint_info["tag"],
                "download_date": datetime.now().isoformat(),
                "total_records": total_records,
                "pages_downloaded": page_count,
                "elapsed_seconds": round(elapsed, 2),
                "status": "in_progress",
            }
            save_data(filepath, all_data, metadata)
            logger.info(f"Checkpoint saved: {total_records} total records")

        if records_in_page < max_limit:
            logger.info(f"Last page reached ({records_in_page} < {max_limit})")
            break

        offset += max_limit
        time.sleep(0.5)

    if all_data:
        elapsed = time.time() - start_time
        metadata = {
            "endpoint": path,
            "summary": endpoint_info["summary"],
            "tag": endpoint_info["tag"],
            "download_date": datetime.now().isoformat(),
            "total_records": total_records,
            "pages_downloaded": page_count,
            "elapsed_seconds": round(elapsed, 2),
            "status": "complete",
        }
        save_data(filepath, all_data, metadata)
        logger.info(f"Completed: {total_records} records in {page_count} pages")
        logger.info(f"File: {filepath}")
        logger.info(f"Time: {elapsed:.2f}s")
        return True
    else:
        logger.warning("No data collected for this endpoint")
        return False


def download_openapi_data(output_dir: str) -> None:
    logger.info("=" * 80)
    logger.info("DOWNLOADING OPEN DATA API OF THE BRAZILIAN MINISTRY OF HEALTH")
    logger.info("=" * 80)

    try:
        openapi_dir = os.path.join(output_dir, "openapi")
        os.makedirs(openapi_dir, exist_ok=True)
        logger.info(f"OpenAPI output directory: {openapi_dir}")
    except Exception as e:
        logger.error(f"Failed to create OpenAPI output directory: {e}")
        return

    try:
        swagger_spec = load_swagger_spec()
        logger.info(
            f"Specification loaded: {swagger_spec['info']['title']} v{swagger_spec['info']['version']}"
        )

        endpoints = extract_endpoints(swagger_spec)
        logger.info(f"Found {len(endpoints)} endpoints for download")

        endpoints_by_tag = {}
        for ep in endpoints:
            tag = ep["tag"]
            if tag not in endpoints_by_tag:
                endpoints_by_tag[tag] = []
            endpoints_by_tag[tag].append(ep)

        logger.info("Endpoints by category:")
        for tag, eps in sorted(endpoints_by_tag.items()):
            logger.info(f"  {tag}: {len(eps)} endpoints")

        logger.warning(f"Data from {len(endpoints)} endpoints will be downloaded.")
        logger.warning("This may take a long time depending on the data volume.")

        logger.info("Starting downloads...")

        successful = 0
        failed = 0

        for i, endpoint_info in enumerate(endpoints, 1):
            logger.info(
                f"[{i}/{len(endpoints)}] Starting download of endpoint: {endpoint_info['path']}"
            )
            if download_endpoint(endpoint_info, openapi_dir):
                successful += 1
            else:
                failed += 1

            time.sleep(1)

        logger.info("=" * 80)
        logger.info(
            f"OpenAPI download completed: {successful} successful, {failed} failed"
        )
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Error downloading OpenAPI data: {e}")
