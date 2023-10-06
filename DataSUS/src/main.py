import requests
from bs4 import BeautifulSoup as soup
import re
import os

def get_page_content(url):
    response = requests.get(url)
    return soup(response.content, "html.parser")

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)

def download_resource(resource_url, save_path):
    if not os.path.exists(save_path):
        response = requests.get(resource_url)
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print("Downloaded...")

def clean_filename(filename):
    return re.sub(r"[\n\t\s]*", "", filename)

def main():
    URL = "https://opendatasus.saude.gov.br/organization/ministerio-da-saude"
    PRE_URL = "https://opendatasus.saude.gov.br"
    mediaPath = "../media/"

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
                        file_type = resource_item.find("span").text

                        if file_type in ["PDF", "XLSX", "XLS", "ODT", "ZIP", "zip csv"]:
                            data = get_page_content(PRE_URL + resource_item["href"])
                            item = data.find("div", class_="btn-group").find("a")

                            response = requests.get(item["href"])
                            if file_type == "zip csv":
                                file_name = clean_filename(resource_item.text) + ".zip"
                            else:
                                file_name = clean_filename(resource_item.text) + "." + file_type.lower()
                            file_path = os.path.join(dataset_path, file_name)
                            download_resource(item["href"], file_path)

                        if file_type == "CSV":
                            data = get_page_content(PRE_URL + resource_item["href"])
                            item = data.find("div", class_="btn-group")
                            if item is not None:
                                response = requests.get(item.find("a")["href"])
                                file_name = clean_filename(resource_item.text) + "." + file_type.lower()
                                file_path = os.path.join(dataset_path, file_name)
                                download_resource(item.find("a")["href"], file_path)
                            else:
                                csvlist = data.find("div", class_="prose notes")
                                for csvlink in csvlist.find_all("a", href=True):
                                    csv_text = clean_filename(csvlink.text)
                                    file_name = csv_text + "." + file_type.lower()
                                    file_path = os.path.join(dataset_path, file_name)
                                    download_resource(csvlink['href'], file_path)

                    print("\n")

if __name__ == "__main__":
    main()