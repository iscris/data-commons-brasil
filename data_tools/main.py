import argparse

from data_tools.data_processor.main import run_processor
from data_tools.downloaders.ipea import ipea
from data_tools.importer.main import run_importer


def handle_downloader_option(option: str):
    if option == "ipea":
        ipea.download_ipea_data()
    else:
        raise NotImplementedError


def main():
    parser = argparse.ArgumentParser(description="")
    subparser = parser.add_subparsers(dest="command", required=True, help="")

    downloader_parser = subparser.add_parser("download", help="")
    downloader_parser.add_argument("--source", type=str, required=True, help="")

    data_processor_parser = subparser.add_parser("format", help="")
    data_processor_parser.add_argument("--input", type=str, required=True, help="")
    data_processor_parser.add_argument("--output", type=str, default="data_processor", help="")
    data_processor_parser.add_argument("--values", type=str, help="")

    importer_parser = subparser.add_parser("import", help="")
    importer_parser.add_argument("--input", type=str, required=True, help="")
    importer_parser.add_argument("--output", type=str, required=True, help="")

    args = parser.parse_args()
    base_output_path = "output" 

    if args.command == "download":
        handle_downloader_option(args.source)
    elif args.command == "data_processor":
        values_path = args.values if args.values else f"{args.input}/values"
        run_processor(args.input, f"{base_output_path}/{args.output}", values_path)
    elif args.command == "import":
        run_importer(args.input, f"{base_output_path}/{args.output}")


if __name__ == "__main__":
    main()
