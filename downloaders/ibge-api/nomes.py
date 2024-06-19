import requests
import json
import time

# Define the endpoint
url = "https://servicodados.ibge.gov.br/api/v2/censos/nomes/ranking/"

nomes_dump = {}
for decada in range(1930,2030,10):
    # nomes_dump[decada] = {} 
    time.sleep(1)
    params = {
        "decada": decada, 
    }
    print("Retrieving decade:", decada)
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the JSON response
        # data = response.json()
        nomes_dump[decada] = response.json()
    else:
        print(f"Failed to retrieve data: {response.status_code}")

with open("dump-nomes.json", "w") as f:
    f.write(json.dumps(nomes_dump))
