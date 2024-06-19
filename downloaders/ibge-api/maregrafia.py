import requests
import json
import time

#retrieving info on maregrafos

maregrafos = requests.get("https://servicodados.ibge.gov.br/api/v1/rmpg/maregrafos").json()
with open("dump-maregrafos.json","w") as f:
    f.write(json.dumps(maregrafos))

codigos_maregrafos = [x["siglaMaregrafo"] for x in maregrafos]
print(codigos_maregrafos)

#retrieving info on sensores

sensores = requests.get("https://servicodados.ibge.gov.br/api/v1/rmpg/sensores").json()
with open("dump-sensores.json","w") as f:
    f.write(json.dumps(sensores))

codigos_sensores = [x["nomeSensor"] for x in sensores]
print(codigos_sensores)

# retrieving info on tide predictions
mares_anual = {}
for codigo in codigos_maregrafos:
    mares_anual[codigo] = {}
    for year in range(2012,2025):
        time.sleep(1)
        print("Maregrafos - Ano",year," - Codigo",codigo)
        url = "https://servicodados.ibge.gov.br/api/v1/rmpg/previsao/"+codigo+"?momentoInicial="+str(year)+"-01-01-00-00&momentoFinal="+str(year)+"-12-31-23-59"
        response = requests.get(url)
        mares_anual[codigo][year] = response.json()

with open("dump-mares-anual.json", "w") as f:
    f.write(json.dumps(mares_anual))


## we will use this as a helper for or mare requests
month_range = {
    "01":"01-31",
    "02":"02-28",
    "03":"03-31",
    "04":"04-30",
    "05":"05-31",
    "06":"06-30",
    "07":"07-31",
    "08":"08-30",
    "09":"09-30",
    "10":"10-31",
    "11":"11-30",
    "12":"12-31",
    }

## crawling previsao de mare

for codigo in codigos_maregrafos:
    nivel_mare_anual = {}
    nivel_mare_anual[codigo] = {}
    for year in range(2012,2025):
        nivel_mare_anual[codigo][year] = {}
        for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
            time.sleep(1)
            print("Mare - Ano",year,"- Mes",month," - Codigo",codigo)
            url = "https://servicodados.ibge.gov.br/api/v1/rmpg/nivel/"+codigo+"?momentoInicial="+str(year)+"-"+month+"-01-10-00&momentoFinal="+str(year)+"-"+month_range[month]+"-13-00&incluirPrevisao=S"
            response = requests.get(url)
            nivel_mare_anual[codigo][year][month] = response.json()
        break
    with open("dump-nivel-mare-mensal.jsonl", "a") as f:
        f.write(json.dumps(nivel_mare_anual)+"\n")

## crawling metereologia


for codigo in codigos_maregrafos:
    metereologia_anual = {}
    metereologia_anual[codigo] = {}
    for year in range(2012,2025):
        metereologia_anual[codigo][year] = {}
        for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
            time.sleep(1)
            print("Mare - Ano",year,"- Mes",month," - Codigo",codigo)
            try:
                url = "https://servicodados.ibge.gov.br/api/v1/rmpg/meteorologia/"+codigo+"?momentoInicial="+str(year)+"-"+month+"-01-10-00&momentoFinal="+str(year)+"-"+month_range[month]+"-13-00"
                response = requests.get(url)
                metereologia_anual[codigo][year][month] = response.json()
            except Exception as e:
                print(e)
                metereologia_anual[codigo][year][month] = {"error":e}
    with open("dump-metereologia-mensal.jsonl", "a") as f:
        f.write(json.dumps(metereologia_anual)+"\n")