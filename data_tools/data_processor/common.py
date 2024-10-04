import logging
import re
import unicodedata
from pathlib import Path

import polars as pl

from data_tools.data_processor import LOG_HANDLER, CsvData

logger = logging.getLogger(LOG_HANDLER)


def read_csv(csv_path: str | Path) -> CsvData:
    return pl.read_csv(csv_path)


def save_csv(data: CsvData, csv_path: Path | str):
    if data.is_empty():
        # raise ValueError("No data to save.")
        logging.info(f"saving empty .csv in path={csv_path}")
    return data.write_csv(csv_path)


def remove_accents(input_str: str):
    nfkd_form = unicodedata.normalize("NFD", input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def compare_strings_ignore_accents(str1: str, str2: str):
    str1_normalized = remove_accents(str1)
    str2_normalized = remove_accents(str2)
    return str1_normalized == str2_normalized


def index_ignore_accents(list_: list[str], value: str):
    value_normalized = remove_accents(value)
    for i, item in enumerate(list_):
        if compare_strings_ignore_accents(item, value_normalized):
            return i

    return None


# FIXME|HACK: This logic does not generalize well. It works just for now. IA would do a better job also
def display_name_into_stat_var_name(name: str):
    stat_var_name = name.strip()

    deprecated_keys = [
        "inativa",
        "descontinuada",
        "descontinuado",
        "discontinued",
        "descontinued",
        "inactive",
    ]

    prefix = ""
    for key in deprecated_keys:
        if re.search(key, stat_var_name, re.IGNORECASE):
            prefix = key.lower()
            stat_var_name = re.sub(key, "", stat_var_name, flags=re.IGNORECASE)
            break

    stat_var_name = re.sub(
        r"[^a-zA-Z0-9\s]", "", stat_var_name
    )  # Remove non-alphanumeric characters except spaces
    stat_var_name = re.sub(
        r"\s+", "_", stat_var_name
    )  # Replace spaces with underscores

    if prefix:
        stat_var_name = f"{stat_var_name}_{prefix}"

    stat_var_name = re.sub(r"_+", "_", stat_var_name)  # Remove multiple underscores

    return stat_var_name.lower().strip("_")
