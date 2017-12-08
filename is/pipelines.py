# -*- coding: utf-8 -*-

from scrapy import signals
from scrapy.exporters import JsonLinesItemExporter, JsonItemExporter
from time import gmtime, strftime
import logging
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

OUTDIR = './output/'

class JsonExportPipeline(object):

    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        timestamp = strftime("%Y%m%d_%H%M%S", gmtime())
        file = open(OUTDIR+'%s%s_products.json' % (timestamp, spider.name), 'w+b')
        #file.write('[')
        self.files[spider] = file
        self.exporter = JsonItemExporter(file,encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()
        self.log("Output file: "+ file)

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files.pop(spider)
        #file.write(']')
        file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class JsonMatchPipeline(object):

    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        timestamp = str(int(time.time()))
        file = open(OUTDIR+'%s%s_ingredients.json' % (timestamp, spider.name), 'w+b')
        #file.write('[')
        self.files[spider] = file
        self.exporter = JsonLinesItemExporter(file,encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files.pop(spider)
        #file.write(']')
        file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item
