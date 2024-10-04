# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import scrapy
from itemadapter import ItemAdapter
from scrapy.pipelines.files import FilesPipeline


class IbgePipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        # Use the original URL as the filename
        custom_name = request.url.replace("https://ftp.ibge.gov.br/", "").replace(
            "/", "##"
        )
        return custom_name.split("/")[-1]

    def get_media_requests(self, item, info):
        adapter = ItemAdapter(item)
        for file_url in adapter["file_urls"]:
            yield scrapy.Request(file_url)
