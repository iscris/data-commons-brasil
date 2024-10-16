import os
import subprocess
from pathlib import Path


def run_importer(input_dir: str, output_dir: str, image: str | None):
    # input_dir = parse_dir(input_dir)
    # output_dir = parse_dir(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    image = image or "datacommons-data:latest"
    run_dc_import_tool_local(input_dir, output_dir, image)


def parse_dir(dir: str):
    if dir.startswith("gs://"):
        # TODO: garantir que termina com "/"
        return dir
    print(dir)
    print(str(dir))

    return Path.cwd() / dir


def run_dc_import_tool_local(input_dir: Path, output_dir: Path, image: str):
    run_dc_import_tool(input_dir, output_dir, image)


def is_gcloud_path(path: Path | str):
    path = path if isinstance(path, str) else str(path)

    return path.startswith("gs://")


def run_dc_import_tool(input_dir: Path, output_dir: Path, image: str):
    current_dir = os.getcwd()
    home_dir = os.path.expanduser("~")

    volumes = []
    if not is_gcloud_path(input_dir):
        volumes.extend(["-v", f"{input_dir}:{input_dir}"])

    if not is_gcloud_path(output_dir):
        volumes.extend(["-v", f"$PWD/{output_dir}:{output_dir}"])

    docker_command = [
        "docker",
        "run",
        "-it",
        "--env-file",
        f"{current_dir}/.env",
        "--env-file",
        f"{current_dir}/.env.qa",
        "-e",
        f"INPUT_DIR={input_dir}",
        "-e",
        f"OUTPUT_DIR={output_dir}",
        "-e",
        "GOOGLE_APPLICATION_CREDENTIALS=/gcp/creds.json",
        "-v",
        f"{home_dir}/.config/gcloud/application_default_credentials.json:/gcp/creds.json",
    ]

    full_cmd = docker_command + volumes + [image]

    result = subprocess.run(full_cmd, capture_output=True, text=True)
    print(result.stdout)

    if result.returncode == 0:
        print("Docker container ran successfully")
    else:
        print(f"Error running Docker container: {result.stderr}")
