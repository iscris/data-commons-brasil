# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class IbgePipeline:
    def process_item(self, item, spider):
        return item

    def file_path(self, request, response=None, info=None, *, item=None):
        # Use the original URL as the filename
        custom_name = request.url.replace("https://ftp.ibge.gov.br/","")
        return custom_name.split("/")[-1]