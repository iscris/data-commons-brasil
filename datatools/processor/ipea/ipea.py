import json
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from traceback import format_exc

from datatools.processor import (
    LOG_HANDLER,
    ContextInfo,
    CsvData,
    FileInfo,
    IpeaInfo,
    ValueFileInfo,
    WorkerData,
)

from .common import read_csv, save_csv
from .config_formatter import make_config_json
from .csv_formatter import update_data
from .data_extractor import extract_data_from_file

logger = logging.getLogger(LOG_HANDLER)


def get_ipea_info(input_path: str) -> IpeaInfo:
    metadata = read_csv(f"{input_path}/metadata.csv")
    themes = read_csv(f"{input_path}/themes.csv")
    territories = read_csv(f"{input_path}/territories.csv")
    countries = read_csv(f"{input_path}/countries.csv")
    latest_updates = read_csv(f"{input_path}/latest_updates.csv")

    return IpeaInfo(metadata, themes, territories, countries, latest_updates)


counter = 0


def worker(data: WorkerData):
    global counter
    logger.info(f"Processing file number: {counter}")
    counter += 1

    full_path = os.path.join(data.values_path, data.file_info.file)

    try:
        value_file_data = extract_data_from_file(full_path, data.context_info)
    except Exception:
        logger.info(f"Error on extracting data from: {full_path}")
        logger.debug(format_exc())
        return

    values_files_data = [value_file_data]
    if value_file_data.has_multilevel_territories:
        logger.info(
            f"The file '{full_path}' has territories with different levels of specifity. Splitting into multiple..."
        )

        splitted_files_info = split_file_by_territory_level(
            full_path,
            value_file_data.stat_var_name,
            value_file_data.group,
            data.context_info,
        )

        values_files_data = splitted_files_info

    contents: list[CsvData] = []
    files_data_to_keep = []
    for idx, f_data in enumerate(values_files_data):
        content = update_data(full_path, f_data, data.context_info)
        if content is None:
            logger.info(f"Skipping file {full_path}")
            continue

        logger.debug(f"[main] The file {full_path} has {f_data=}")
        contents.append(content)
        files_data_to_keep.append(values_files_data[idx])

    assert len(files_data_to_keep) == len(contents)

    return contents, files_data_to_keep


def split_file_by_territory_level(
    file_path: str | Path, stat_var_name: str, theme: str, context_info: ContextInfo
) -> list[ValueFileInfo]:
    file_path = Path(file_path)

    if isinstance(context_info, IpeaInfo):
        code = file_path.stem
        file_data = read_csv(file_path)

        levels = file_data["NIVNOME"].unique()

        level_file_infos: list[ValueFileInfo] = []
        for level in levels:
            level_file_name = f"{level}_{file_path.name}"

            level_file_infos.append(
                ValueFileInfo(
                    file_name=level_file_name,
                    code=code,
                    group=theme,
                    stat_var_name=stat_var_name,
                    level_info=level,
                    has_multilevel_territories=True,
                )
            )

        return level_file_infos

    else:
        raise NotImplementedError


def process_files(path: str, output_path: str, context_info: ContextInfo):
    files = os.listdir(path)

    args = [
        WorkerData(path, output_path, FileInfo(file), context_info) for file in files
    ]

    extracted_data = []
    csv_content = []

    counter = 1
    n_log_steps = len(files) // 2
    log_step = len(files) // max(n_log_steps, 1)

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(worker, arg): arg for arg in args}

        for future in as_completed(futures):
            result = future.result()
            logger.debug(f"Number of processed files: {counter} / {len(files)}")
            if counter % log_step == 0 or counter == 1:
                logger.info(f"Number of processed files: {counter} / {len(files)}")

            counter += 1
            if result is not None:
                content, value_file_data = result
                extracted_data.extend(value_file_data)
                csv_content.extend(content)

    assert len(extracted_data) == len(csv_content)

    return extracted_data, csv_content


def process_ipea_data(input_path: str, output_path: str, values_path: str):
    logger.info(f"Reading files from: {input_path}")
    logger.info(f"The raw data is in: {values_path}")
    logger.info(f"Writing processed files to: {output_path}")

    context_info = get_ipea_info(input_path)
    csv_files_data, csv_files_content = process_files(
        values_path, output_path, context_info
    )

    json_file_data = make_config_json(csv_files_data, context_info)

    shutil.rmtree(output_path, ignore_errors=True)
    os.makedirs(output_path, exist_ok=True)
    for file_content, file_data in zip(csv_files_content, csv_files_data):
        file_name = file_data.file_name
        path = Path(output_path) / file_name

        logger.debug(
            f"[main] Saving file {file_name} with {file_data.stat_var_name=} at path {path}"
        )
        save_csv(file_content, path)

    with open(f"{output_path}/config.json", "w+") as f:
        json.dump(json_file_data, f)
