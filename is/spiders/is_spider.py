# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import logging
import urlparse
import os
import json
import time
import re
import string
import regex

OUTDIR = './output/'


class MpreisSpider(scrapy.Spider):
    name = "ms"

    custom_settings = {
                       'ITEM_PIPELINES': {
                       'is.pipelines.JsonExportPipeline': 300
                                         }
                      }


    def __init__(self, baseURL=None, *args, **kwargs):
        super(MpreisSpider, self).__init__(*args, **kwargs)
        self.baseURL = baseURL
        if not os.path.exists(OUTDIR):
            try:
                os.makedirs(OUTDIR)
            except OSError:
                raise

    def start_requests(self):
        urls = [self.baseURL]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        splitUrl = response.url.rstrip('/').split('/')
        #self.log(splitUrl)           
        if re.match(r'Lebensmittel',splitUrl[-1]):
            xp = '//a[@class="toplevel current" and \
                  text()="Lebensmittel"]/following::ul[@class="sublevel2"] \
                  /li/a[contains(@href,"Lebensmittel")]/@href'
            for url in response.xpath(xp).extract():
                yield response.follow(url, callback=self.parse)        
        else:
            xpp='//h2[@class="tm-title"]/../@href'
            prods = response.xpath(xpp).extract()
            if re.match(r'\d',splitUrl[-1]):
                next_page = str(int(splitUrl[-1])+1)
                if len(prods)==20 : # not last page
                    yield response.follow('/'.join(splitUrl[:-1])+'/'+next_page, callback=self.parse)
            if not splitUrl[-1].endswith('#lmiv') and not re.match(r'\d',splitUrl[-1]): # page 1
                yield response.follow('/'.join(splitUrl)+'/'+'2', callback=self.parse)
            for p in prods:
                #self.log('request prod '+p+'#lmiv') 
                yield response.follow(p+'#lmiv', callback=self.parse_prod)       
            

    def parse_prod(self, response):
        data = {}
        xp = '//h1[@id="productTitle"]/span/text()' 
        data["name"] = response.xpath(xp).extract_first()
        data["stores"] = ["Mpreis"]
        tb = response.xpath('(//div[@id="cs0"]/div[1]/table[1]/tr)')
        for i in range(len(tb)/2):
            k = response.xpath('(//div[@id="cs0"]/div[1]/table[1]/tr['+str(i)+']/th/text())').extract_first()
            v = response.xpath('(//div[@id="cs0"]/div[1]/table[1]/tr['+str(i)+']/td/text())').extract_first()
            data[k] = v
        if "Marke" in data:
            data["brand"] = data["Marke"]
        data["labels"] = []
        if u'Zutaten' in data:
            ingredients = string.replace(data[u'Zutaten'],'Zutaten: ','').rstrip('.,')
            ingredients = re.sub('(?<=\d),(?=\d)', '.',ingredients)
            r = regex.compile(r',\s*(?![^\(\)]*\))\s*')
            ingredients = r.split(ingredients)
            ingredients = filter(lambda v: v is not None, ingredients)
            # remove duplicate ingredients  
            seen = set()
            seen_add = seen.add
            data["ingredients"] = [x for x in ingredients if not (x in seen or seen_add(x))]
        else:
            data["ingredients"] = []
        data["details"] = {}
        data["details"]["size"] = {}
        xp = '//div[@class="tm-content"]/text()'
        size = response.xpath(xp).extract()[0].lstrip().split()[:2]
        if len(size) >0:
            m = re.match(r'\d+[,]*\d*',size[0])
            if m:
                s = m.group()
                data["details"]["size"]["amount"] = float(re.sub('(?<=\d),(?=\d)', '.',s))
            if len(size) >1:
                data["details"]["size"]["unit"] = size[1].lower()
        data["details"]["price"] = {}
        xp = '//label[@id="productPrice"]/strong/span[1]/text()[1]'
        price = response.xpath(xp).extract()[0]
        xp = '//label[@id="batchPrice"]/strong/span[1]/span/text()'
        price = price+''.join(response.xpath(xp).extract())
        data["details"]["price"]["amount"] = float(re.sub('(?<=\d),(?=\d)', '.',price))
        data["details"]["price"]["currency"] = "EUR"   
        #self.log(data)
        yield data



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
                unit = unit.lower()
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

