import os
import shutil
from uuid import uuid4

from .ipea.ipea import process_ipea_data


def pick_processor(source: str, input_path: str, tmp_output_path: str):
    match source:
        case "ipea":
            process_ipea_data(input_path, tmp_output_path, f"{input_path}/values")
        case _:
            raise NotImplementedError


# Nothing bellow this comment needs to be modified when adding
# a new processing source
def process_source(source: str, input_path: str, output_path: str):
    process_id = uuid4()
    # A temporary path is created to make sure that errors in processing
    # won't result in losing the last valid output
    tmp_path = f"{output_path}/tmp_{process_id}"
    final_path = f"{output_path}/{source}"
    pick_processor(source, input_path, tmp_path)

    shutil.rmtree(final_path)
    os.makedirs(final_path, exist_ok=True)
    move_content(tmp_path, final_path)
    print("cheguei aqui 2")
    os.rmdir(tmp_path)
    print(f"Download concluded ({final_path})")


def move_content(source_dir: str, destination: str):
    for file in os.listdir(source_dir):
        shutil.move(os.path.join(source_dir, file), destination)
