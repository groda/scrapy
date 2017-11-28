# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import logging
import urlparse
import os
import json
import time

OUTDIR = './output/'

class InterSpider(scrapy.Spider):
    name = "is"

    def __init__(self, baseURL=None, prodURL=None, *args, **kwargs):
        super(InterSpider, self).__init__(*args, **kwargs)
        self.baseURL = baseURL
	self.prodURL = prodURL
        if not os.path.exists(OUTDIR):
            try:
	        os.makedirs(OUTDIR)
            except OSError:
	        raise

    def start_requests(self):
        opts = '&rank=prod-rank&sp_cs=UTF-8' 
        urls = [self.baseURL+opts]
	for i in range(2,16): #last page +1 = 1654 --> range(2,1654)
            urls.append(self.baseURL+'&page='+str(i)+opts)
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        splitUrl = urlparse.parse_qs(response.url)
	if 'page' in splitUrl:
            page = splitUrl['page'][0]
	else:
	    page = 1
        filename = 'lebensmittel-%s.txt' % page
        #self.log("Encoding: "+ response.encoding)
        #with open(os.path.join(OUTDIR,filename), 'wb') as f:
        #    f.write(response.body)
        data = response.body.split('],  "results" :')[1]
        data = data.split('"applied-filters" :')[0].strip().rstrip(',')
	data=json.loads(data)
        self.log('Saved file %s' % filename)
        keys = ["name", "brand", "stores", "producer", "labels", "ingredients", "details"]
	# product-short-description-2 --> name
	# product-short-description-3 --> details.size.amount details.size.unit
	# product-price --> details.price.amount
	# product-image --> details.image_url
	newdata = []
        for p in data:
            newp = {}
            newp["name"] = p["product-short-description-2"]
            newp["code"] = p["code"]
            newp["url"] = self.prodURL+p["code"]
            newp["brand"] = ""
            newp["stores"] =  ["Interspar"]
            newp["producer"] = ""
            newp["labels"] = []
            newp["ingredients"] = []
	    desc3 = p["product-short-description-3"].split()
	    if len(desc3)>1:
                [amount, unit] = desc3[:2]
	        amount = float(amount.replace(',','.'))
            else:
	        [amount, unit] = [0, ""]
            newp["details"] = {"size": {"amount": amount, "unit": unit},
                               "price": {"amount": float(p["product-price"].replace(',','.')), 
	                                 "currency": "EUR"},
                               "image_url": p["product-image"]
		              }
            newdata.append(newp)
	return newdata


class InterSpiderIng(scrapy.Spider):
    name = "is_ing"
    
    custom_settings = {
                       'ITEM_PIPELINES': {
		       'is.pipelines.JsonMatchPipeline': 300
		                         }
	              }
    def __init__(self, baseURL=None, codesFile=None, *args, **kwargs):
        super(InterSpiderIng, self).__init__(*args, **kwargs)
        self.baseURL = baseURL
	self.codesFile = codesFile


    def start_requests(self):
        # read codes from json file codesFile
        with open(self.codesFile, 'r') as json_file:
	    data = json.load(json_file)
        codes = [p["code"] for p in data]	
	#self.log("List of codes:")
        #self.log(codes)
        for code in codes:
            url = self.baseURL+code
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
	code = response.url.split('/')[-1]
        filename = 'ingredients-%s.txt' % code
	xp = '//div[@class="pdpTabsRow"]/div[@class="titleColumn pdpTabsColumn"]/label[@class="title" and contains(string(),"Zutaten")]/following::ul[@class="descColumn pdpTabsColumn"][1]/li[@class="desc"]/text()'
        self.log("Encoding: "+ response.encoding)
        #with open(os.path.join(OUTDIR,filename), 'wb') as f:
        #    f.write(response.body)
	ingredients = response.xpath(xp).extract()
	if len(ingredients)>0:
	    result = {code: ingredients[0]}
	else: 
            result = {code: []}
	self.log(result)

        yield result

