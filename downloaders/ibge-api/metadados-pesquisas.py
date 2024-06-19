import requests
import json 
import time
SLEEP_TIME = 1
# Retrieving all pesquisas

pesquisas_url = "http://servicodados.ibge.gov.br/api/v2/metadados/pesquisas" 

response = requests.get(pesquisas_url)
data = response.json()
with open("dump-pesquisas-geral.json", "w") as f:
    f.write(json.dumps(data))
codigos_pesquisa = [x["codigo"] for x in data]
print(codigos_pesquisa)

# retrieving periodos
codigos_dump = {}
for codigo in codigos_pesquisa:
    print("Retrieving pesquisa:", codigo)
    time.sleep(SLEEP_TIME)
    url = "http://servicodados.ibge.gov.br/api/v2/metadados/pesquisas/"+str(codigo)+"/periodos"
    response = requests.get(url)
    if response.status_code == 200:
        try:
            codigos_dump[codigo] = response.json()
        except Exception as e:
            print("Could not retrieve code",codigo,"check error:",e) 
    else:
        print(f"Failed to retrieve data: {response.status_code}")
    

with open("dump-frequencia-pesquisas.json", "w") as f:
    f.write(json.dumps(codigos_dump))


# retrieving metadata for each pesquisa
# we know from dump-frequencia-pesquisas that the first year any pesquisa happened was 1994.


for codigo in codigos_pesquisa:
    time.sleep(SLEEP_TIME)
    codigo_dump_dict = {}
    codigo_dump_dict["codigo"] = codigo
    codigo_dump_dict["anos"] = {}
    for year in range(1994,2023):
        time.sleep(SLEEP_TIME)
        url = "http://servicodados.ibge.gov.br/api/v2/metadados/"+codigo+"/"+str(year)+"/"
        response = requests.get(url)
        if response.status_code == 200:
            try:
                codigo_dump_dict["anos"][year] = response.json()
            except Exception as e:
                print(e)
        else:
            print(f"Failed to retrieve data: {response.status_code}")
            print(response.json())
    with open("dump-metadados-pesquisas.jsonl", "a+") as f:
        f.write(json.dumps(codigo_dump_dict)+"\n")
