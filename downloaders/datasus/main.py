import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup as soup
import time
import re
import os

def get_page_content(url, max_retries=5, delay_seconds=20):
    retries = 0
    while retries < max_retries:
        response = requests.get(url)
        content = soup(response.content, "html.parser")

        if content is not None:
            return content

        print(f"Retrying {retries + 1}/{max_retries} after {delay_seconds} seconds...")
        time.sleep(delay_seconds)
        retries += 1

    print(f"Max retries reached. Unable to retrieve content from {url}")
    return None

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)

def download_resource(resource_url, save_path, basic = ""):
    if not os.path.exists(save_path):
        if not basic == "":
            response = requests.get(resource_url, auth=basic)
        else:
            response = requests.get(resource_url)
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print("Downloaded...")
    else:
        print("Already Downloaded")

def clean_filename(filename):
    return re.sub(r"[\n\t\s/]*", "", filename)

def main():
    URL = "https://opendatasus.saude.gov.br/organization/ministerio-da-saude"
    PRE_URL = "https://opendatasus.saude.gov.br"
    PRE_URL_API = "https://apidadosabertos.saude.gov.br"
    mediaPath = "downloads/"
    create_directory(mediaPath)
    usuario = None
    senha = None


    content = get_page_content(URL)
    pages = content.find_all("ul", class_="pagination")

    for page in pages:
        links = page.find_all("a")
        for link in links:
            if link.text != "»":
                print(f"Page {link.text}\n")
                link_url = link["href"]
                page = get_page_content(PRE_URL + link_url)
                datasets = page.find_all("li", class_="dataset-item")

                for dataset in datasets:
                    heading = dataset.find("h3", class_="dataset-heading").find("a")
                    print(heading.text)
                    dataset_path = os.path.join(mediaPath, heading.text)
                    create_directory(dataset_path)

                    data = get_page_content(PRE_URL + heading["href"])
                    resources = data.find_all("li", class_="resource-item")
        
                    for resource in resources:
                        resource_item = resource.find("a")
                        print(resource_item.text)
                        file_type = resource_item.find("span").text.lower()
                        if file_type in ["pdf", "xlsx", "xls", "odt", "zip", "zip csv", "api"]:
                            data = get_page_content(PRE_URL + resource_item["href"])
                            search_button = data.find("div", class_="btn-group")
                            if search_button is not None:
                                item = search_button.find("a")
                                if file_type == "zip csv":
                                    file_name = clean_filename(resource_item.text) + ".zip"
                                    file_path = os.path.join(dataset_path, file_name)                         
                                    download_resource(item["href"], file_path)
                                if file_type == "api":                                   
                                    paragraph_tags = data.find_all('p')
                                    for tag in paragraph_tags:
                                        text = tag.get_text()
                                        if "Usuário:" in text:
                                            usuario = text.split("Usuário:")[1].strip()
                                        elif "Senha:" in text:
                                            senha = text.split("Senha:")[1].strip()                                
                                                                       
                                    
                                    if usuario is None or senha is None:
                                        apidata = get_page_content(item["href"])
                                        script_tags = apidata.find_all('script')
                                        
                                        user_config = None

                                        for script_tag in script_tags:
                                            javascript_code = script_tag.string if script_tag else None

                                            if javascript_code and 'var user_config' in javascript_code:
                                                lines = javascript_code.split('\n')

                                                for line in lines:
                                                    if 'var user_config' in line:
                                                        url_start = line.find('"url": "') + len('"url": "')
                                                        url_end = line.find('"', url_start)
                                                        url = line[url_start:url_end]

                                        item["href"] = PRE_URL_API + url
                                        basic = ""
                                    else:
                                        basic = HTTPBasicAuth(usuario,senha)
                                    
                                    file_name = clean_filename(resource_item.text) + ".json"
                                    file_path = os.path.join(dataset_path, file_name)                         
                                    download_resource(item["href"], file_path, basic)
                                    usuario = None
                                    senha = None
                                else:
                                    file_name = clean_filename(resource_item.text) + "." + file_type
                                    file_path = os.path.join(dataset_path, file_name)                         
                                    download_resource(item["href"], file_path)
                                                      
                        if file_type == "csv":
                            data = get_page_content(PRE_URL + resource_item["href"])
                            item = data.find("div", class_="btn-group")
                            if item is not None:
                                response = requests.get(item.find("a")["href"])
                                file_name = clean_filename(resource_item.text) + "." + file_type
                                file_path = os.path.join(dataset_path, file_name)
                                download_resource(item.find("a")["href"], file_path)
                            else:
                                csvlist = data.find("div", class_="prose notes")
                                if csvlist is None:
                                    retries = 0
                                    while retries < 5:
                                        time.sleep(20)
                                        data = get_page_content(PRE_URL + resource_item["href"])
                                        csvlist = data.find("div", class_="prose notes")
                                        if csvlist is not None:
                                            retries = 5
                                            for csvlink in csvlist.find_all("a", href=True):
                                                csv_text = clean_filename(csvlink.text)
                                                file_name = csv_text + "." + file_type
                                                file_path = os.path.join(dataset_path, file_name)
                                                download_resource(csvlink['href'], file_path)
                                        else:
                                            retries += 1
                                    print("Not possible to download")
                                else:
                                    for csvlink in csvlist.find_all("a", href=True):
                                        csv_text = clean_filename(csvlink.text)
                                        file_name = csv_text + "." + file_type
                                        file_path = os.path.join(dataset_path, file_name)
                                        download_resource(csvlink['href'], file_path)
                    print("\n")

if __name__ == "__main__":
    main()
    while True:
        pass