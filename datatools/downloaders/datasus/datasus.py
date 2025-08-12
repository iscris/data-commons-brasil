import logging
import os
import re
import time

import requests
from bs4 import BeautifulSoup as soup
from requests.auth import HTTPBasicAuth

logger = logging.getLogger("downloader: DATASUS")
logging.basicConfig(level=logging.INFO)

FILE_FORMAT_PRIORITY = [
    "api",
    "csv",
    "json",
    "zip csv",
    "xlsx",
    "xls",
    "zip",
    "pdf",
    "odt",
]


def get_page_content(url, max_retries=5, delay_seconds=20):
    retries = 0
    while retries < max_retries:
        try:
            logger.debug(
                f"Making request to: {url} (attempt {retries + 1}/{max_retries})"
            )
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            content = soup(response.content, "html.parser")

            if content is not None:
                logger.debug(f"Successfully retrieved content from {url}")
                return content

        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {url}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error parsing content from {url}: {e}")

        retries += 1
        if retries < max_retries:
            logger.debug(f"Retrying after {delay_seconds} seconds...")
            time.sleep(delay_seconds)

    logger.error(f"Max retries reached. Unable to retrieve content from {url}")
    return None


def download_resource(resource_url, save_path, basic=""):
    try:
        if os.path.exists(save_path):
            logger.info(f"File already exists: {save_path}")
            return True

        logger.info(f"Downloading: {resource_url} to {save_path}")

        if basic != "":
            response = requests.get(resource_url, auth=basic, timeout=120)
        else:
            response = requests.get(resource_url, timeout=120)

        response.raise_for_status()

        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        with open(save_path, "wb") as file:
            file.write(response.content)

        logger.info(f"Successfully downloaded: {save_path}")
        return True

    except requests.exceptions.Timeout:
        logger.error(f"Timeout downloading {resource_url}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {resource_url}: {e}")
        return False
    except IOError as e:
        logger.error(f"Failed to save file {save_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading {resource_url}: {e}")
        return False


def clean_filename(filename):
    return re.sub(r"[\n\t\s/]*", "", filename)


def get_highest_priority_format(resources):
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
                    or priority < resource_formats[file_type]["priority"]
                ):
                    resource_formats[file_type] = {
                        "resource": resource,
                        "priority": priority,
                        "name": resource_name,
                        "type": file_type,
                    }
        except Exception as e:
            logger.warning(f"Error processing resource: {e}")
            continue

    if not resource_formats:
        return None

    best_format = min(resource_formats.values(), key=lambda x: x["priority"])
    return best_format


def download_datasus_data(output_dir: str):
    URL = "https://opendatasus.saude.gov.br/organization/ministerio-da-saude"
    PRE_URL = "https://opendatasus.saude.gov.br"
    PRE_URL_API = "https://apidadosabertos.saude.gov.br"

    try:
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Output directory: {output_dir}")
    except Exception as e:
        logger.error(f"Failed to create output directory {output_dir}: {e}")
        return

    content = get_page_content(URL)
    if not content:
        logger.error("Failed to get main page content")
        return

    page_buttons = content.find_all("li", class_="page-item")
    len_page_buttons = len(page_buttons)
    logger.info(f"Found {len_page_buttons} page(s)")

    processed_datasets = set()

    for page_num in range(1, len_page_buttons):
        curr_page_url = URL + f"?page={page_num}"
        content = get_page_content(curr_page_url)
        if not content:
            logger.error(f"Failed to get content for page {page_num}")
            continue

        try:
            datasets = content.find_all("li", class_="dataset-item")
            logger.info(f"Found {len(datasets)} datasets on page {page_num}")

            for dataset in datasets:
                try:
                    heading = dataset.find("h2", class_="dataset-heading")
                    if not heading:
                        logger.warning("Dataset heading not found")
                        continue

                    heading_link = heading.find("a")
                    if not heading_link:
                        logger.warning("Dataset heading link not found")
                        continue

                    dataset_name = clean_filename(heading_link.text)

                    if dataset_name in processed_datasets:
                        logger.info(f"Dataset already processed: {dataset_name}")
                        continue

                    processed_datasets.add(dataset_name)
                    logger.info(f"Processing dataset: {dataset_name}")

                    data = get_page_content(PRE_URL + heading_link["href"])
                    if not data:
                        logger.warning(f"Failed to get dataset page for {dataset_name}")
                        continue

                    resources = data.find_all("li", class_="resource-item")
                    if not resources:
                        logger.warning(f"No resources found for dataset {dataset_name}")
                        continue

                    best_resource = get_highest_priority_format(resources)
                    if not best_resource:
                        logger.warning(
                            f"No suitable resources found for dataset {dataset_name}"
                        )
                        continue

                    resource = best_resource["resource"]
                    resource_name = best_resource["name"]
                    file_type = best_resource["type"]

                    logger.info(
                        f"Selected format '{file_type}' for dataset {dataset_name}"
                    )

                    try:
                        resource_item = resource.find("a")
                        resource_page = get_page_content(
                            PRE_URL + resource_item["href"]
                        )

                        if not resource_page:
                            logger.warning(
                                f"Failed to get resource page for {resource_name}"
                            )
                            continue

                        download_btn = resource_page.find("div", class_="btn-group")
                        if not download_btn:
                            logger.warning(
                                f"Download button not found for {resource_name}"
                            )
                            continue

                        item = download_btn.find("a")
                        if not item:
                            logger.warning(
                                f"Download link not found for {resource_name}"
                            )
                            continue

                        if file_type == "zip csv":
                            file_name = f"{dataset_name}_{resource_name}.zip"
                            file_path = os.path.join(output_dir, file_name)
                            # Construir URL completa se for relativa
                            download_url = item["href"]
                            if download_url.startswith("/"):
                                download_url = PRE_URL + download_url
                            download_resource(download_url, file_path)

                        elif file_type == "api":
                            usuario = None
                            senha = None

                            paragraph_tags = resource_page.find_all("p")
                            for tag in paragraph_tags:
                                try:
                                    text = tag.get_text()
                                    if "Usuário:" in text:
                                        usuario = text.split("Usuário:")[1].strip()
                                    elif "Senha:" in text:
                                        senha = text.split("Senha:")[1].strip()
                                except Exception as e:
                                    logger.warning(f"Error parsing credentials: {e}")

                            basic = ""
                            if usuario is None or senha is None:
                                try:
                                    apidata = get_page_content(item["href"])
                                    if apidata:
                                        script_tags = apidata.find_all("script")

                                        for script_tag in script_tags:
                                            javascript_code = (
                                                script_tag.string
                                                if script_tag
                                                else None
                                            )

                                            if (
                                                javascript_code
                                                and "var user_config" in javascript_code
                                            ):
                                                lines = javascript_code.split("\n")

                                                for line in lines:
                                                    if "var user_config" in line:
                                                        url_start = line.find(
                                                            '"url": "'
                                                        ) + len('"url": "')
                                                        url_end = line.find(
                                                            '"', url_start
                                                        )
                                                        if (
                                                            url_start > 7
                                                            and url_end > url_start
                                                        ):
                                                            url = line[
                                                                url_start:url_end
                                                            ]
                                                            item["href"] = (
                                                                PRE_URL_API + url
                                                            )
                                                            break
                                except Exception as e:
                                    logger.warning(f"Error extracting API URL: {e}")
                            else:
                                basic = HTTPBasicAuth(usuario, senha)

                            file_name = f"{dataset_name}_{resource_name}.json"
                            file_path = os.path.join(output_dir, file_name)
                            download_resource(item["href"], file_path, basic)

                        elif file_type == "csv":
                            csvlist = resource_page.find("div", class_="prose notes")
                            if csvlist:
                                csv_links = csvlist.find_all("a", href=True)
                                if csv_links:
                                    csvlink = csv_links[0]
                                    csv_text = clean_filename(csvlink.text)
                                    file_name = f"{dataset_name}_{resource_name}_{csv_text}.{file_type}"
                                    file_path = os.path.join(output_dir, file_name)
                                    # Construir URL completa se for relativa
                                    download_url = csvlink["href"]
                                    if download_url.startswith("/"):
                                        download_url = PRE_URL + download_url
                                    download_resource(download_url, file_path)
                                else:
                                    logger.warning(
                                        f"No CSV links found for {resource_name}"
                                    )
                            else:
                                file_name = (
                                    f"{dataset_name}_{resource_name}.{file_type}"
                                )
                                file_path = os.path.join(output_dir, file_name)
                                # Construir URL completa se for relativa
                                download_url = item["href"]
                                if download_url.startswith("/"):
                                    download_url = PRE_URL + download_url
                                download_resource(download_url, file_path)
                        else:
                            file_name = f"{dataset_name}_{resource_name}.{file_type}"
                            file_path = os.path.join(output_dir, file_name)
                            # Construir URL completa se for relativa
                            download_url = item["href"]
                            if download_url.startswith("/"):
                                download_url = PRE_URL + download_url
                            download_resource(download_url, file_path)

                    except Exception as e:
                        logger.error(
                            f"Error processing resource {resource_name} in dataset {dataset_name}: {e}"
                        )
                        continue

                except Exception as e:
                    logger.error(f"Error processing dataset: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error processing page: {e}")
            continue

    logger.info("DATASUS data download completed")
