import argparse

from datatools.downloaders.downloaders import download_from_source
from datatools.processor.processor import process_source


def main():
    parser = argparse.ArgumentParser(description="")
    subparser = parser.add_subparsers(dest="command", required=True, help="")

    downloader_parser = subparser.add_parser("download", help="")
    downloader_parser.add_argument("--source", type=str, required=True, help="")
    downloader_parser.add_argument("--skip", type=str, help="")
    downloader_parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers for downloading (default: 4)",
    )

    processor_parser = subparser.add_parser("process", help="")
    processor_parser.add_argument("--source", type=str, required=True, help="")
    processor_parser.add_argument("--input", type=str, required=True, help="")

    args = parser.parse_args()
    base_output_path = "output"

    if args.command == "download":
        download_from_source(
            args.source, "output/downloader", args.skip, args.workers
        )
    elif args.command == "process":
        process_source(args.source, args.input, f"{base_output_path}/processor")
    else:
        raise NotImplementedError(f"Operation not implemented: {args.command}")


if __name__ == "__main__":
    main()
