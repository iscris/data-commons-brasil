import logging
import os
import traceback
from typing import Set

from .http_client import download_resource, get_page_content
from .openapi import download_openapi_data
from .parser import (
    extract_dataset_info_from_page,
    get_highest_priority_resource,
    get_resource_page_and_link,
)
from .types import URL, DatasetInfo, DownloadContext

logger = logging.getLogger("downloader:DATASUS")
logging.basicConfig(level=logging.INFO)


def process_dataset(dataset_info: DatasetInfo, output_dir: str) -> None:
    logger.info(f"Processing dataset: {dataset_info.name}")

    resources = dataset_info.page_content.find_all("li", class_="resource-item")
    if not resources:
        logger.warning(f"No resources found for dataset {dataset_info.name}")
        return

    best_resource = get_highest_priority_resource(resources)
    if not best_resource:
        logger.warning(f"No suitable resources found for dataset {dataset_info.name}")
        return

    logger.info(
        f"Selected format '{best_resource.type}' for dataset {dataset_info.name}"
    )

    resource_page, download_item = get_resource_page_and_link(best_resource.resource)

    if not resource_page or not download_item:
        logger.warning(f"Failed to get download link for {best_resource.name}")
        return

    ctx = DownloadContext(
        dataset_name=dataset_info.name,
        resource_name=best_resource.name,
        file_type=best_resource.type,
        output_dir=output_dir,
        resource_page=resource_page,
        download_item=download_item,
        dataset_info=dataset_info,
    )

    try:
        download_resource(ctx)
    except Exception as e:
        logger.error(f"Error downloading resource {best_resource.name}: {e}")
        logger.error(traceback.format_exc())


def download_datasus_data(output_dir: str) -> None:
    try:
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Output directory: {output_dir}")
    except Exception as e:
        logger.error(f"Failed to create output directory {output_dir}: {e}")
        return

    datasets_listing_page = get_page_content(URL)
    if not datasets_listing_page:
        logger.error("Failed to get datasets listing page content")
        return

    total_pages = len(datasets_listing_page.find_all("li", class_="page-item"))

    logger.info(f"Found {total_pages} page(s)")

    processed_datasets: Set[str] = set()

    for page_num in range(1, total_pages):
        datasets_info = extract_dataset_info_from_page(page_num)

        for dataset_info in datasets_info:
            if dataset_info.name in processed_datasets:
                logger.info(f"Dataset already processed: {dataset_info.name}")
                continue

            processed_datasets.add(dataset_info.name)

            try:
                process_dataset(dataset_info, output_dir)
            except Exception as e:
                logger.error(f"Error processing dataset {dataset_info.name}: {e}")

    logger.info("DATASUS dataset download completed")

    logger.info("Starting OpenAPI data download...")
    try:
        download_openapi_data(output_dir)
    except Exception as e:
        logger.error(f"Error during OpenAPI download: {e}")

    logger.info("All DATASUS data download completed")
