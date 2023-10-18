import concurrent.futures
import os

import ipeadatapy


def get_values(code: str):
    return ipeadatapy.api_call(
        f"http://www.ipeadata.gov.br/api/odata4/Metadados('{code}')/Valores"
    )


os.makedirs("downloads", exist_ok=True)
ipeadatapy.metadata().to_csv("downloads/metadata.csv", index=False)
ipeadatapy.countries().to_csv("downloads/countries.csv", index=False)
ipeadatapy.latest_updates().to_csv("downloads/latest_updates.csv", index=False)
ipeadatapy.sources().to_csv("downloads/sources.csv", index=False)
ipeadatapy.themes().to_csv("downloads/themes.csv", index=False)
ipeadatapy.territories().to_csv("downloads/territories.csv", index=False)

df = ipeadatapy.list_series()
code_list = df["CODE"].tolist()

with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
    results = executor.map(get_values, code_list)
    for result in results:
        code = result.iloc[0]["SERCODIGO"]
        os.makedirs("downloads/values", exist_ok=True)
        result.to_csv(f"downloads/values/{code}.csv")
