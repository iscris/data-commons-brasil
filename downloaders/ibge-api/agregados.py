import requests
import json
import time
from tqdm import tqdm

request = requests.get("https://servicodados.ibge.gov.br/api/v3/agregados")
data = request.json()

## retrieving agregados dump
with open("dump-agregados.json", "w") as f:
    f.write(json.dumps(data))


agregados_ids = []
for agg_ids in data:
    for i in agg_ids["agregados"]:
        agregados_ids.append(i["id"])

# crawling metadados

with open("dump-metadados-agregados.jsonl","a+") as f:
    for i in tqdm(agregados_ids):
        time.sleep(1)
        url = "https://servicodados.ibge.gov.br/api/v3/agregados/"+i+"/metadados"
        try:
            mtdt = requests.get(url).json()
            f.write(json.dumps(mtdt)+"\n")
        except Exception as e:
            print("Error in agregado",i,":",e)

with open("dump-periodos-agregados.jsonl","a+") as f:
    for i in tqdm(agregados_ids):
        time.sleep(1)
        url = "https://servicodados.ibge.gov.br/api/v3/agregados/"+i+"/periodos"
        try:
            mtdt = requests.get(url).json()
            dump_dict = {}
            dump_dict["id"] = i
            dump_dict["periodos"] = mtdt 
            f.write(json.dumps(dump_dict)+"\n")
        except Exception as e:
            print("Error in agregado",i,":",e)