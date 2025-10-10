import logging
from typing import List, Optional

from bs4 import BeautifulSoup

from .http_client import clean_filename, get_page_content
from .types import FILE_FORMAT_PRIORITY, PRE_URL, URL, DatasetInfo, ResourceInfo

logger = logging.getLogger("downloader: DATASUS")


def get_highest_priority_resource(resources: List[any]) -> Optional[ResourceInfo]:
    resource_formats = {}

    for resource in resources:
        try:
            resource_item = resource.find("a")
            if not resource_item:
                continue

            span = resource_item.find("span")
            if not span:
                continue

            file_type = span.text.lower().strip()
            resource_name = clean_filename(resource_item.text)

            if file_type in FILE_FORMAT_PRIORITY:
                priority = FILE_FORMAT_PRIORITY.index(file_type)
                if (
                    file_type not in resource_formats
                    or priority < resource_formats[file_type].priority
                ):
                    resource_formats[file_type] = ResourceInfo(
                        resource=resource,
                        priority=priority,
                        name=resource_name,
                        type=file_type,
                    )
        except Exception as e:
            logger.warning(f"Error processing resource: {e}")
            continue

    if not resource_formats:
        return None

    return min(resource_formats.values(), key=lambda x: x.priority)


def parse_dataset_item(dataset: any) -> Optional[DatasetInfo]:
    try:
        heading = dataset.find("h2", class_="dataset-heading")
        if not heading:
            return None

        heading_link = heading.find("a")
        if not heading_link:
            return None

        dataset_name = clean_filename(heading_link.text)
        dataset_url = PRE_URL + heading_link["href"]

        data = get_page_content(dataset_url)
        if not data:
            logger.warning(f"Failed to get dataset page for {dataset_name}")
            return None

        return DatasetInfo(name=dataset_name, url=dataset_url, page_content=data)

    except Exception as e:
        logger.error(f"Error parsing dataset: {e}")
        return None


def extract_dataset_info_from_page(page_num: int) -> List[DatasetInfo]:
    curr_page_url = URL + f"?page={page_num}"
    content = get_page_content(curr_page_url)

    if not content:
        logger.error(f"Failed to get content for page {page_num}")
        return []

    datasets = content.find_all("li", class_="dataset-item")
    logger.info(f"Found {len(datasets)} datasets on page {page_num}")

    dataset_infos = []
    for dataset in datasets:
        dataset_info = parse_dataset_item(dataset)
        if dataset_info:
            dataset_infos.append(dataset_info)

    return dataset_infos


def get_resource_page_and_link(
    resource: any,
) -> tuple[Optional[BeautifulSoup], Optional[any]]:
    try:
        resource_item = resource.find("a")
        resource_page = get_page_content(PRE_URL + resource_item["href"])

        if not resource_page:
            return None, None

        download_btn = resource_page.find("div", class_="btn-group")
        if not download_btn:
            return None, None

        item = download_btn.find("a")
        if not item:
            return None, None

        return resource_page, item

    except Exception as e:
        logger.error(f"Error getting resource page: {e}")
        return None, None
