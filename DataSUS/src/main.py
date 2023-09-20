import requests
from bs4 import BeautifulSoup as soup
import re
import os

URL = "https://opendatasus.saude.gov.br/organization/ministerio-da-saude"
PRE_URL = "https://opendatasus.saude.gov.br"
initialPage = requests.get(URL)
mediaPath = "../media/"

content = soup(initialPage.content, "html.parser").find(id="content")
pages = content.find_all("ul", class_="pagination")
for page in pages:
    links = page.find_all("a")
    for link in links:
        if link.text != "»":
            print(f"Page {link.text}\n")         
            link_url = link["href"]
            page = requests.get(PRE_URL+link_url)
            datasets = soup(page.content, "html.parser").find_all("li", class_="dataset-item")
            for dataset in datasets:
                heading = dataset.find("h3", class_="dataset-heading").find("a")                
                print(heading.text)
                if not os.path.exists(mediaPath+heading.text):
                    os.makedirs(mediaPath+heading.text)
                data = requests.get(PRE_URL+heading["href"])
                resources = soup(data.content, "html.parser").find_all("li", class_="resource-item")
                for resource in resources:
                    resource_item = resource.find("a")
                    print(resource_item.text)
                    if resource_item.find("span").text == "PDF":                    
                        data = requests.get(PRE_URL+resource_item["href"])
                        item = soup(data.content, "html.parser").find("div", class_="btn-group").find("a") 
                        
                        response = requests.get(item["href"])
                        pdf = open(mediaPath+heading.text+"/"+str(re.sub(r"[\n\t\s]*", "", resource_item.text))+".pdf", 'wb')
                        pdf.write(response.content)
                        pdf.close()
                        
                        print("Downloaded...")
                    if resource_item.find("span").text == "CSV":
                        data = requests.get(PRE_URL+resource_item["href"])
                        item = soup(data.content, "html.parser").find("div", class_="btn-group")
                        if item is not None:
                            response = requests.get(item.find("a")["href"])
                            csv = open(mediaPath+heading.text+"/"+str(re.sub(r"[\n\t\s]*", "", resource_item.text))+".csv", 'wb')
                            csv.write(response.content)
                            csv.close()
                            
                            print("Downloaded...")
                        else:
                            
                print("\n")

#datasets = content.find_all("li", class_="dataset-item")
#for dataset in datasets:
#    print(dataset, end="\n"*2)


