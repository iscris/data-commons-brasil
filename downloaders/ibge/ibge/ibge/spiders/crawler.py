import scrapy
from pathlib import Path
from ibge.items import IbgeItem
    

class IBGESpider(scrapy.Spider):
    name = "ibge"
    
    start_urls = [
        "https://ftp.ibge.gov.br/Indices_de_Precos_Consumidor_Harmonizado/",
    ]

    def parse(self, response):
        page = response.url.split("/")[-2]
        # filename = f"{page}.html"
        # Path(filename).write_bytes(response.body)
        # self.log(response.url.split("/"))
        # self.log(f"Saved file {filename}")

        if response.url:
            item = IbgeItem()
            item['file_urls'] = [response.url]
            yield item

        new_links = []
        for quote in response.css("table"):
            for row in quote.css("a"):
                new_links.append(row.css("a::attr(href)").extract()[0])
                
        for new in new_links:
            yield response.follow(url=new, callback=self.parse)