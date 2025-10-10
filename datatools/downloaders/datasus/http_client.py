import logging
import os
import re
import time
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth

from .types import (
    MAX_RETRIES,
    OPEN_API_URL,
    PRE_URL,
    PRE_URL_API,
    REQUEST_TIMEOUT,
    RETRY_DELAY_SECONDS,
    DownloadContext,
)

logger = logging.getLogger("downloader: DATASUS")


def clean_filename(filename: str) -> str:
    return re.sub(r"[\n\t\s/]*", "", filename)


def build_full_url(url: str) -> str:
    if url.startswith("/"):
        return PRE_URL + url
    return url


def get_page_content(
    url: str, max_retries: int = MAX_RETRIES, delay_seconds: int = RETRY_DELAY_SECONDS
) -> Optional[BeautifulSoup]:
    retries = 0
    while retries < max_retries:
        try:
            logger.debug(
                f"Making request to: {url} (attempt {retries + 1}/{max_retries})"
            )
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            content = BeautifulSoup(response.content, "html.parser")

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


def download_file(
    url: str, save_path: str, auth: Optional[HTTPBasicAuth] = None
) -> bool:
    if os.path.exists(save_path):
        logger.info(f"File already exists: {save_path}")
        return True

    retries = 0
    while retries < MAX_RETRIES:
        try:
            logger.info(
                f"Downloading: {url} to {save_path} (attempt {retries + 1}/{MAX_RETRIES})"
            )

            if auth is not None:
                response = requests.get(url, auth=auth, timeout=REQUEST_TIMEOUT)
            else:
                response = requests.get(url, timeout=REQUEST_TIMEOUT)

            response.raise_for_status()
            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            with open(save_path, "wb") as file:
                file.write(response.content)

            logger.info(f"Successfully downloaded: {save_path}")
            return True

        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {url}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")

        retries += 1
        if retries < MAX_RETRIES:
            logger.debug(f"Retrying after {RETRY_DELAY_SECONDS} seconds...")
            time.sleep(RETRY_DELAY_SECONDS)

    logger.error(f"Max retries reached. Unable to download {url}")
    return False


def extract_api_credentials(
    resource_page: BeautifulSoup,
) -> tuple[Optional[str], Optional[str]]:
    usuario = None
    senha = None

    for tag in resource_page.find_all("p"):
        try:
            text = tag.get_text()
            if "Usuário:" in text:
                usuario = text.split("Usuário:")[1].strip()
            elif "Senha:" in text:
                senha = text.split("Senha:")[1].strip()
        except Exception as e:
            logger.warning(f"Error parsing credentials: {e}")

    return usuario, senha


def extract_api_url_from_javascript(item: any) -> Optional[str]:
    try:
        apidata = get_page_content(item["href"])
        if not apidata:
            return None

        for script_tag in apidata.find_all("script"):
            javascript_code = script_tag.string if script_tag else None

            if javascript_code and "var user_config" in javascript_code:
                for line in javascript_code.split("\n"):
                    if "var user_config" in line:
                        url_start = line.find('"url": "') + len('"url": "')
                        url_end = line.find('"', url_start)
                        if url_start > 7 and url_end > url_start:
                            url = line[url_start:url_end]
                            return PRE_URL_API + url
    except Exception as e:
        logger.warning(f"Error extracting API URL: {e}")

    return None


def handle_api_download(ctx: DownloadContext) -> None:
    api_link = ctx.resource_page.select_one("div.row.wrapper > section > div > p > a")

    if OPEN_API_URL in api_link["href"]:
        logger.info("Skipping ")
        return
    else:
        return handle_basic_api_download(ctx)


def handle_basic_api_download(ctx: DownloadContext) -> None:
    usuario, senha = extract_api_credentials(ctx.resource_page)
    auth = None
    if usuario is None or senha is None:
        api_url = extract_api_url_from_javascript(ctx.download_item)
        if api_url:
            ctx.download_item["href"] = api_url
    else:
        auth = HTTPBasicAuth(usuario, senha)

    file_name = f"{ctx.dataset_name}_{ctx.resource_name}.json"
    file_path = os.path.join(ctx.output_dir, file_name)
    download_file(ctx.download_item["href"], file_path, auth)


def create_api_function(endpoint_path: str, spec: dict[str, Any], base_url: str):
    def api_call(**kwargs):
        endpoint = f"{base_url.rstrip('/')}{endpoint_path}"

        method_spec = spec.get("get", {})
        parameters = method_spec.get("parameters", [])

        params = {}
        headers = {}

        formato = kwargs.pop("formato", "json")

        for param in parameters:
            param_name = param["name"]
            if param_name in kwargs:
                params[param_name] = kwargs[param_name]
            elif "default" in param:
                params[param_name] = param["default"]

        produces = method_spec.get("produces", ["application/json"])
        if formato == "csv" and "text/csv" in produces:
            headers["Accept"] = "text/csv"
        else:
            headers["Accept"] = "application/json"

        retries = 0
        while retries < MAX_RETRIES:
            try:
                response = requests.get(
                    endpoint, params=params, headers=headers, timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()

                if headers["Accept"] == "text/csv":
                    return response.text
                else:
                    return response.json()

            except requests.exceptions.RequestException as e:
                logger.error(
                    f"API request error (attempt {retries + 1}/{MAX_RETRIES}): {e}"
                )
                retries += 1
                if retries < MAX_RETRIES:
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    return None

        return None

    return api_call


def find_all_download_links(ctx: DownloadContext, file_type: str) -> list[dict]:
    resource_pages_links = []
    resource_items = ctx.dataset_info.page_content.find_all(
        "li", class_="resource-item"
    )
    for item in resource_items:
        heading_link = item.find("a", class_="heading", href=True)
        if heading_link:
            label = heading_link.find("span").get_text()
            if file_type.lower() in label.lower():
                resource_pages_links.append(heading_link)

    download_links = []
    for link in resource_pages_links:
        href = link.get("href", "")
        resource_page = get_page_content(PRE_URL + href)
        if resource_page:
            download_link = resource_page.select_one(
                "div.row.wrapper > section > div > p > a"
            )
            download_link_href = download_link.get("href")
            if download_link_href:
                download_links.append(download_link_href)

    return download_links


def handle_file_download(ctx: DownloadContext) -> None:
    file_extension = "zip" if ctx.file_type == "zip csv" else ctx.file_type

    all_links = find_all_download_links(ctx, file_extension)
    logger.info(f"Found {len(all_links)} download links")

    if all_links:
        for idx, link in enumerate(all_links):
            link_text = clean_filename(link)
            suffix = f"_{link_text}" if link_text else f"_{idx}"
            file_name = (
                f"{ctx.dataset_name}_{ctx.resource_name}{suffix}.{file_extension}"
            )
            file_path = os.path.join(ctx.output_dir, file_name)
            download_url = build_full_url(link)
            download_file(download_url, file_path)
    else:
        file_name = f"{ctx.dataset_name}_{ctx.resource_name}.{file_extension}"
        file_path = os.path.join(ctx.output_dir, file_name)
        download_url = build_full_url(ctx.download_item["href"])
        download_file(download_url, file_path)


def download_resource(ctx: DownloadContext) -> None:
    if ctx.file_type == "api":
        handle_api_download(ctx)
    else:
        handle_file_download(ctx)
