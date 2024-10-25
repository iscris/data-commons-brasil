import concurrent.futures
import os
import traceback

import ipeadatapy
import pandas as pd


def get_values(code: str) -> tuple[bool, pd.DataFrame | None, str | None]:
    try:
        values = ipeadatapy.api_call(
            f"http://www.ipeadata.gov.br/api/odata4/Metadados('{code}')/Valores"
        )

        return True, values, None

    except Exception as e:
        return False, None, traceback.format_exception(e)


def save_file(path: str, code: str, data: pd.DataFrame) -> tuple[bool, str | None]:
    try:
        os.makedirs(f"{path}/values", exist_ok=True)
        data.to_csv(f"{path}/values/{code}.csv")

        return True, None

    except Exception as e:
        return False, traceback.format_exception(e)


def process_code(save_path: str, code: str) -> tuple[bool, str | None]:
    is_values_ok, data_df, values_error = get_values(code)
    if not is_values_ok:
        return False, values_error

    is_save_ok, save_error = save_file(save_path, code, data_df)
    if not is_save_ok:
        return False, save_error

    return True, None


def download_ipea_data(output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    ipeadatapy.metadata().to_csv(f"{output_dir}/metadata.csv", index=False)
    ipeadatapy.countries().to_csv(f"{output_dir}/countries.csv", index=False)
    ipeadatapy.latest_updates().to_csv(f"{output_dir}/latest_updates.csv", index=False)
    ipeadatapy.sources().to_csv(f"{output_dir}/sources.csv", index=False)
    ipeadatapy.themes().to_csv(f"{output_dir}/themes.csv", index=False)
    ipeadatapy.territories().to_csv(f"{output_dir}/territories.csv", index=False)

    df = ipeadatapy.list_series()
    code_list = df["CODE"].tolist()

    with open("logs.txt", "w+") as log_file:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(
                lambda code: process_code(output_dir, code), code_list
            )
            for result in results:
                is_success, error = result
                if error:
                    log_file.write(f"{error}\n")
