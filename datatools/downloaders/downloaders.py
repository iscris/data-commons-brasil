import os
import shutil
from datetime import datetime
from uuid import uuid4

from .ipea.ipea import download_ipea_data


def pick_downloader(source: str, download_path: str):
    match source:
        case "ipea":
            download_ipea_data(download_path)
        case _:
            raise NotImplementedError


def download_from_source(source: str, download_path: str):
    download_id = uuid4()
    tmp_path = f"{download_path}/{source}/tmp_{download_id}"
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