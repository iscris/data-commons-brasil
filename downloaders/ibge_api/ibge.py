import json
import os

import requests


def get_values(localidade):
    r = requests.get(
        f"https://servicodados.ibge.gov.br/api/v1/localidades/{localidade}"
    )

    if r.status_code != 200:
        print("Falha na requisição: Status", r.status_code)
        return {}

    return r.json()


localidades = [
    "distritos",
    "mesorregioes",
    "microrregioes",
    "municipios",
    "paises",
    "regioes",
    "regioes-imediatas",
    "regioes-integradas-de-desenvolvimento",
    "regioes-metropolitanas",
    "subdistritos",
    "estados",
]

os.makedirs("downloads", exist_ok=True)
for localidade in localidades:
    localidade_json = get_values(localidade)
    file_path = f"downloads/{localidade}.json"

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(localidade_json, file, ensure_ascii=False, indent=4)