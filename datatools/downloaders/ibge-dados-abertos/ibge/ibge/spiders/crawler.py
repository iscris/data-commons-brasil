import scrapy

from ibge.items import IbgeItem


class IBGESpider(scrapy.Spider):
    name = "ibge"

    start_urls = [
        "https://ftp.ibge.gov.br/",
    ]

    def parse(self, response):
        content_type = response.headers.get("Content-Type", b"").decode("utf-8").lower()

        if (
            "ftp.ibge.gov.br" in response.url
        ):  # follows only links within ibge's ftp server
            if "text/html" in content_type:
                new_links = []
                for quote in response.css("table"):
                    for row in quote.css("a"):
                        new_links.append(row.css("a::attr(href)").extract()[0])

                for new in new_links:
                    yield response.follow(url=new, callback=self.parse)
            else:
                with open("files_url.txt", "a+") as f:
                    f.write(response.url + "\n")

                item = IbgeItem()
                item["file_urls"] = [response.url]
                yield item
