from dataclasses import dataclass

from bs4 import BeautifulSoup

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

URL = "https://opendatasus.saude.gov.br/organization/ministerio-da-saude"
PRE_URL = "https://opendatasus.saude.gov.br"
PRE_URL_API = "https://apidadosabertos.saude.gov.br"
OPEN_API_URL = "https://apidadosabertos.saude.gov.br/"

MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 20
REQUEST_TIMEOUT = 30


@dataclass
class ResourceInfo:
    resource: any
    priority: int
    name: str
    type: str


@dataclass
class DatasetInfo:
    name: str
    url: str
    page_content: BeautifulSoup


@dataclass
class DownloadContext:
    dataset_name: str
    resource_name: str
    file_type: str
    output_dir: str
    resource_page: BeautifulSoup
    download_item: any
    dataset_info: DatasetInfo
