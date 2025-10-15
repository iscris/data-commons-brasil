import logging
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Set

from .http_client import download_resource, get_page_content
from .openapi import download_openapi_data
from .parser import (
    extract_dataset_info_from_page,
    get_highest_priority_resource,
    get_resource_page_and_link,
)
from .progress_tracker import ProgressTracker
from .types import URL, DatasetInfo, DownloadContext

logger = logging.getLogger("downloader:DATASUS")
logging.basicConfig(level=logging.INFO)


def process_dataset(
    dataset_info: DatasetInfo, output_dir: str, progress_tracker: ProgressTracker
) -> tuple[str, bool]:
    """
    Process a single dataset.
    Returns: (dataset_name, success)
    """
    dataset_name = dataset_info.name

    # Check if already completed
    if progress_tracker.is_completed(dataset_name):
        logger.info(f"Skipping already completed dataset: {dataset_name}")
        return (dataset_name, True)

    # Check if files already exist (from interrupted download)
    if progress_tracker.verify_dataset_files(dataset_name, output_dir):
        logger.info(f"Files already exist for dataset: {dataset_name}, marking as completed")
        progress_tracker.mark_completed(dataset_name)
        return (dataset_name, True)

    logger.info(f"Processing dataset: {dataset_name}")

    resources = dataset_info.page_content.find_all("li", class_="resource-item")
    if not resources:
        logger.warning(f"No resources found for dataset {dataset_name}")
        return (dataset_name, False)

    best_resource = get_highest_priority_resource(resources)
    if not best_resource:
        logger.warning(f"No suitable resources found for dataset {dataset_name}")
        return (dataset_name, False)

    logger.info(
        f"Selected format '{best_resource.type}' for dataset {dataset_name}"
    )

    resource_page, download_item = get_resource_page_and_link(best_resource.resource)

    if not resource_page or not download_item:
        logger.warning(f"Failed to get download link for {best_resource.name}")
        return (dataset_name, False)

    ctx = DownloadContext(
        dataset_name=dataset_name,
        resource_name=best_resource.name,
        file_type=best_resource.type,
        output_dir=output_dir,
        resource_page=resource_page,
        download_item=download_item,
        dataset_info=dataset_info,
    )

    try:
        success = download_resource(ctx)
        if success:
            progress_tracker.mark_completed(dataset_name)
            return (dataset_name, True)
        else:
            logger.error(f"Download failed for {dataset_name}")
            return (dataset_name, False)
    except Exception as e:
        logger.error(f"Error downloading resource {best_resource.name}: {e}")
        logger.error(traceback.format_exc())
        return (dataset_name, False)


def download_datasus_data(output_dir: str, max_workers: int = 4) -> None:
    """
    Download DATASUS data with parallel processing support.

    Args:
        output_dir: Directory to save downloaded files
        max_workers: Number of parallel workers (default: 4)
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Using {max_workers} parallel workers")
    except Exception as e:
        logger.error(f"Failed to create output directory {output_dir}: {e}")
        return

    # Initialize progress tracker
    progress_tracker = ProgressTracker(output_dir)
    stats = progress_tracker.get_stats()
    logger.info(f"Progress tracker initialized. Previously completed: {stats['completed_datasets']} datasets")

    datasets_listing_page = get_page_content(URL)
    if not datasets_listing_page:
        logger.error("Failed to get datasets listing page content")
        return

    total_pages = len(datasets_listing_page.find_all("li", class_="page-item"))
    logger.info(f"Found {total_pages} page(s)")

    # Collect all datasets info first
    all_datasets_info = []
    seen_dataset_names: Set[str] = set()

    for page_num in range(1, total_pages):
        logger.info(f"Collecting datasets from page {page_num}/{total_pages}")
        datasets_info = extract_dataset_info_from_page(page_num)

        for dataset_info in datasets_info:
            if dataset_info.name in seen_dataset_names:
                logger.info(f"Duplicate dataset found: {dataset_info.name}, skipping")
                continue

            seen_dataset_names.add(dataset_info.name)
            all_datasets_info.append(dataset_info)

    logger.info(f"Total unique datasets to process: {len(all_datasets_info)}")

    # Process datasets in parallel
    success_count = 0
    failure_count = 0
    skipped_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_dataset = {
            executor.submit(
                process_dataset, dataset_info, output_dir, progress_tracker
            ): dataset_info
            for dataset_info in all_datasets_info
        }

        # Process completed tasks
        for future in as_completed(future_to_dataset):
            dataset_info = future_to_dataset[future]
            try:
                dataset_name, success = future.result()
                if success:
                    success_count += 1
                else:
                    failure_count += 1

                # Log progress
                total_processed = success_count + failure_count
                logger.info(
                    f"Progress: {total_processed}/{len(all_datasets_info)} "
                    f"(Success: {success_count}, Failed: {failure_count})"
                )

            except Exception as e:
                logger.error(f"Error processing dataset {dataset_info.name}: {e}")
                logger.error(traceback.format_exc())
                failure_count += 1

    logger.info("=" * 80)
    logger.info("DATASUS dataset download completed")
    logger.info(f"Total datasets: {len(all_datasets_info)}")
    logger.info(f"Successfully downloaded: {success_count}")
    logger.info(f"Failed: {failure_count}")
    logger.info("=" * 80)

    logger.info("Starting OpenAPI data download...")
    try:
        download_openapi_data(output_dir)
    except Exception as e:
        logger.error(f"Error during OpenAPI download: {e}")

    logger.info("All DATASUS data download completed")
