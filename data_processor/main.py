from pathlib import Path
import subprocess
import argparse
import os


def run():
    parser = argparse.ArgumentParser(description="Processador de dados.")
    parser.add_argument("--input-dir", default="downloads", help="Pasta com arquivos formatados")
    parser.add_argument("--output-dir", default="output", help="Pasta com outputs gerados")
    args = parser.parse_args()

    input_dir = Path.cwd()/args.input_dir
    output_dir = Path.cwd()/args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    run_dc_import_tool_local(input_dir, output_dir)

def run_dc_import_tool_local(input_dir: Path, output_dir: Path):
    run_dc_import_tool("latest", input_dir, output_dir, "datacommons-data")
    
def run_dc_import_tool(version, input_dir, output_dir, image_path):
    current_dir = os.getcwd() 
    home_dir = os.path.expanduser("~") 

    docker_command = [
        "docker", "run",
        "-it",
        "--env-file", f"{current_dir}/env.list",
        "-e", f"INPUT_DIR={input_dir}",
        "-e", f"OUTPUT_DIR={output_dir}",
        "-v", f"{input_dir}:{input_dir}",
        "-v", f"{output_dir}:{output_dir}",
        "-e", "GOOGLE_APPLICATION_CREDENTIALS=/gcp/creds.json",
        "-v", f"{home_dir}/.config/gcloud/application_default_credentials.json:/gcp/creds.json:ro",
        f"{image_path}:{version}"
    ]

    result = subprocess.run(docker_command, capture_output=True, text=True)
    print(result.stdout)

    if result.returncode == 0:
        print("Docker container ran successfully")
    else:
        print(f"Error running Docker container: {result.stderr}")
