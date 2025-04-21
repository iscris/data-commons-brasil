import os
import shutil
from datetime import datetime
from uuid import uuid4

from .ipea.ipea import download_ipea_data
from .ibge.localidades import download_ibge_localidades
from .ibge.agregados import download_ibge_agregados


# In this function, download_path represents a temporary directory.
# This directory will only exist until the download completes and the
# data is transferred to its final destination—a permanent directory
# named with a unique identifying timestamp.
def pick_downloader(source: str, tmp_download_path: str):
    match source:
        case "ipea":
            download_ipea_data(tmp_download_path)
        case "ibge_localidades":
            download_ibge_localidades(tmp_download_path)
        case "ibge_agregados":
            download_ibge_agregados(tmp_download_path)
        case _:
            raise NotImplementedError


# Nothing bellow this comment needs to be modified when adding
# a new downloading source
def download_from_source(source: str, download_path: str):
    download_id = uuid4()
    tmp_path = f"{download_path}/{source}/tmp_{download_id}"
    os.makedirs(tmp_path, exist_ok=True)
    pick_downloader(source, tmp_path)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_path = f"{download_path}/{source}/{timestamp}/"

    os.makedirs(final_path, exist_ok=True)
    move_content(tmp_path, final_path)
    os.rmdir(tmp_path)
    print(f"Download concluded ({final_path})")


def move_content(source_dir: str, destination: str):
    for file in os.listdir(source_dir):
        shutil.move(os.path.join(source_dir, file), destination)
