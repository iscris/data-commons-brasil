import json
import logging
import os
import time

import requests

logger = logging.getLogger("downloader: IBGE agregados")
logging.basicConfig(level=logging.DEBUG)

DC_PLACES = ["N1", "N3", "N6"]
MAX_CALLS_PER_INTERVAL = 120
INTERVAL = 60


def make_api_call(url: str):
    retries = 0
    max_retries = 5
    while retries < max_retries:
        try:
            logger.debug(f"Making API call: {url} {retries=}")
            r = requests.get(url, timeout=120)
            if r.status_code == 200:
                logger.debug("Request concluded")
                return r.json()
        except requests.exceptions.Timeout:
            logger.error("Request timeout")
        except Exception as e:
            logger.error(f"Request failed: Status {e}")

        retries += 1
        logger.debug(f"Retrying after sleep time: {retries=}")
        time.sleep(120)

    return {}


def get_aggregates() -> list[str]:
    aggregates_json = make_api_call("https://servicodados.ibge.gov.br/api/v3/agregados")
    return [
        aggregate["id"]
        for item in aggregates_json
        for aggregate in item.get("agregados", [])
    ]


def get_aggregates_metadata(aggregate_id: str) -> dict:
    return make_api_call(
        f"https://servicodados.ibge.gov.br/api/v3/agregados/{aggregate_id}/metadados"
    )


def get_values(aggregate_id: str, variable_id: str, place_id: str):
    return make_api_call(
        f"https://servicodados.ibge.gov.br/api/v3/agregados/{aggregate_id}/periodos/-100/variaveis/{variable_id}?localidades={place_id}"
    )


def process_and_save_aggregate(aggregate_id: str, output_dir: str):
    logger.info(f"Processing aggregate ID: {aggregate_id}")

    output_path = os.path.join(output_dir, f"{aggregate_id}.json")
    if os.path.exists(output_path):
        logger.info(f"Aggregate {aggregate_id} already exists. Skipping.")
        return

    metadata = get_aggregates_metadata(aggregate_id)
    if not metadata:
        logger.warning(f"Empty metadata for aggregate {aggregate_id}")
        return

    variables_ids = [item["id"] for item in metadata.get("variaveis", [])]
    defined_places: list[str] = metadata.get("nivelTerritorial", {}).get(
        "Administrativo", []
    )
    places_to_search = [place for place in defined_places if place in DC_PLACES]

    series = []
    for variable_id in variables_ids:
        for place in places_to_search:
            values = get_values(aggregate_id, variable_id, place)
            if values:
                series.append(values)

    result = {
        "assunto": metadata.get("assunto", ""),
        "values": series,
    }

    with open(output_path, "w+", encoding="utf-8") as file:
        json.dump(result, file, ensure_ascii=False)
    logger.info(f"Saved aggregate {aggregate_id} to {output_path}")


def download_ibge_agregados(output_dir: str, skip_files: list[str]):
    os.makedirs(output_dir, exist_ok=True)
    aggregate_ids = get_aggregates()
    for aggregate_id in aggregate_ids:
        if f"{aggregate_id}.json" in skip_files:
            logger.info(f"File already exists: {aggregate_id=}.")
            continue

        process_and_save_aggregate(aggregate_id, output_dir)
