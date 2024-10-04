import logging
from pathlib import Path

import polars as pl

from data_tools.data_processor import LOG_HANDLER, IpeaInfo, ValueFileInfo
from data_tools.data_processor.common import read_csv
from data_tools.data_processor.gpt_interface import GptInterface

gpt_interface = GptInterface()
logger = logging.getLogger(LOG_HANDLER)


def extract_data_from_file(file_path: str, context_info: IpeaInfo) -> ValueFileInfo:
    code = Path(file_path).stem
    name = context_info.metadata.filter(pl.col("CODE") == code)["NAME"][0]

    theme_id = context_info.metadata.filter(pl.col("CODE") == code)["THEME CODE"][0]
    theme = context_info.themes.filter(pl.col("ID") == theme_id)["NAME"][0]

    csv_data = read_csv(file_path)

    has_multilevel_territories = (
        "NIVNOME" in csv_data.columns and len(csv_data["NIVNOME"].unique()) > 1
    )

    stat_var_name, groups = get_stat_var_name_and_groups(name, theme)

    logger.debug(
        f"[data_extractor] The file at path: {file_path} has {stat_var_name=} and {name=}"
    )

    return ValueFileInfo(
        code=code,
        file_name=f"{code}.csv",
        stat_var_name=stat_var_name,
        group=groups,
        has_multilevel_territories=has_multilevel_territories,
    )


# TODO: Improve and add IA
def get_stat_var_name_and_groups(original_name: str, original_theme: str):
    return original_name, original_theme
