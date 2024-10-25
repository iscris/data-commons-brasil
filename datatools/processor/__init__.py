import logging
from dataclasses import dataclass
from typing import Literal

import os
import polars as pl

MODULE_PATH = os.path.dirname(__file__)
UTILS_PATH = os.path.join(MODULE_PATH, "utils")

LOG_HANDLER = "datacommons"
logger = logging.getLogger(LOG_HANDLER)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("debug.log", mode="w")
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

log_format = logging.Formatter("[%(name)s] [%(asctime)s] [%(levelname)s]: %(message)s")

file_handler.setFormatter(log_format)
console_handler.setFormatter(log_format)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


@dataclass
class ValueFileInfo:
    file_name: str
    code: str
    group: str
    stat_var_name: str
    level_info: str = "Country"
    has_multilevel_territories: bool = False


CsvData = pl.DataFrame

IpeaMetadataFiles = Literal[
    "metadata", "themes", "territories", "countries", "latest_updates"
]

IpeaMetadata = dict[IpeaMetadataFiles, CsvData]


@dataclass
class FileInfo:
    file: str


@dataclass
class IpeaInfo:
    metadata: CsvData
    themes: CsvData
    territories: CsvData
    countries: CsvData
    latest_updates: CsvData


ContextInfo = IpeaInfo  # | AnyOtherFutureDataSource


@dataclass
class WorkerData:
    values_path: str
    output_path: str
    file_info: FileInfo
    context_info: ContextInfo
