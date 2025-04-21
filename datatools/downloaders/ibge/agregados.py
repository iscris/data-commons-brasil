import requests
import json
import logging
from ratelimit import limits, sleep_and_retry

logger = logging.getLogger("downloader: IBGE agregados")

# N1 - Brasil; N3 - Unidade da Federação; N6 - Município
DC_PLACES = ["N1", "N3", "N6"]
MAX_CALLS_PER_INTERVAL = 120
INTERVAL = 60


@sleep_and_retry
@limits(calls=MAX_CALLS_PER_INTERVAL, period=INTERVAL)
def make_api_call(url: str):
    logger.debug(f"Making API call: {url}")
    r = requests.get(url)

    if r.status_code != 200:
        logger.error(f"Falha na requisição: Status {r.status_code}")
        logger.debug(f"Response: {r.content}")
        return {}

    return r.json()


def get_aggregates() -> dict:
    return make_api_call("https://servicodados.ibge.gov.br/api/v3/agregados")


def get_aggregates_metadata(aggregate_id: str) -> dict:
    return make_api_call(
        f"https://servicodados.ibge.gov.br/api/v3/agregados/{aggregate_id}/metadados"
    )


def get_values(aggregate_id: str, variable_id: str, place_id: str):
    return make_api_call(
        f"https://servicodados.ibge.gov.br/api/v3/agregados/{aggregate_id}/periodos/-100/variaveis/{variable_id}?localidades={place_id}"
    )


def download_ibge_agregados(output_dir: str) -> dict[str, dict[str, str]]:
    def process_aggregate_metadata(
        aggregate: dict[str, str | dict[str, list[str]]],
    ) -> dict[str, str]:
        variables_ids = [item["id"] for item in aggregate.get("variaveis")]

        defined_places: list[str] = aggregate["nivelTerritorial"]["Administrativo"]
        places_to_search = [place for place in defined_places if place in DC_PLACES]
        series = []
        for variable_id in variables_ids:
            for place in places_to_search:
                values = get_values(aggregate["id"], variable_id, place)
                series.append(values)

        return {"assunto": aggregate.get("assunto", ""), "values": series}

    aggregates_json = get_aggregates()
    aggregates_ids = [
        aggregate["id"]
        for item in aggregates_json
        for aggregate in item.get("agregados", [])
    ]

    aggregates_metadata = {id: get_aggregates_metadata(id) for id in aggregates_ids}

    aggregates_cleaned = {
        id: process_aggregate_metadata(aggregate)
        for id, aggregate in aggregates_metadata.items()
    }

    for aggregate_id, metadata in aggregates_cleaned.items():
        with open(f"{output_dir}/{aggregate_id}.json", "w+", encoding="utf-8") as file:
            json.dump(metadata, file, ensure_ascii=False)
