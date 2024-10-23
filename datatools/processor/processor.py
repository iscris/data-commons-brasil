from .ipea.ipea import process_ipea_data


def process_source(source: str, input_path: str, output_path: str):
    match source:
        case "ipea":
            process_ipea_data(input_path, output_path, f"{input_path}/values")
        case _:
            raise NotImplementedError
