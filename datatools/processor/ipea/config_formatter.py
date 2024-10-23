from collections import defaultdict

import polars as pl

from datatools.processor import ContextInfo, ValueFileInfo
from datatools.processor.ipea.common import display_name_into_stat_var_name


def make_config_json(
    value_data_list: list[ValueFileInfo], context_info: ContextInfo
) -> dict:
    config_json = {}

    codes = [val.code for val in value_data_list]
    file_names = [val.file_name for val in value_data_list]
    levels = [val.level_info for val in value_data_list]
    stat_var_names = [val.stat_var_name for val in value_data_list]
    themes = [val.group for val in value_data_list]

    config_json["inputFiles"] = __format_input_files_json(
        codes, levels, file_names, context_info.metadata
    )
    config_json["variables"] = __format_variables_json(
        codes, stat_var_names, context_info.metadata, themes
    )
    config_json["sources"] = __format_sources_json(codes, context_info)

    return config_json


def __format_sources_json(codes: list[str], context_info: ContextInfo) -> dict:
    metadata = context_info.metadata

    config_sources = defaultdict(dict)
    config_sources["IPEA"]["url"] = "http://ipeadata.gov.br"
    config_sources["IPEA"]["provenances"] = {}

    filtered_metadata = metadata.filter(pl.col("CODE").is_in(codes))
    for source, source_url in zip(
        filtered_metadata["SOURCE"], filtered_metadata["SOURCE URL"]
    ):
        config_sources["IPEA"]["provenances"][source] = source_url

    return config_sources


def __format_variables_json(
    codes: list[str],
    stats_var_names: list[str],
    metadata: pl.DataFrame,
    themes: list[str],
) -> dict:
    config_variables = defaultdict(dict)

    for code, stat_var_name, theme in zip(codes, stats_var_names, themes):
        description = metadata.filter(pl.col("CODE") == code)["COMMENT"][0]
        var_code = display_name_into_stat_var_name(stat_var_name)

        config_variables[var_code]["name"] = stat_var_name
        config_variables[var_code]["description"] = description
        config_variables[var_code]["group"] = theme

    return config_variables


def __format_input_files_json(
    codes: list[str], levels: list[str], file_names: list[str], metadata: pl.DataFrame
) -> dict:
    config_input_files = defaultdict(dict)

    level_mapping = {
        "Brasil": "Country",
        "Municípios": "City",
        "Estados": "AdministrativeArea1",
        "Country": "Country",
    }

    for input_file, code, level in zip(file_names, codes, levels):
        provenance = metadata.filter(pl.col("CODE") == code)["SOURCE URL"][0]
        config_input_files[input_file]["provenance"] = provenance
        config_input_files[input_file]["entityType"] = level_mapping.get(level, None)

    return config_input_files
