# -*- coding: utf-8 -*-
import os
import glob
import json
import string
from re import split
import regex

DIR='./output/'
proFile=sorted(filter(os.path.isfile, glob.glob(DIR+'*products*.json')), key=os.path.getmtime, reverse=True)[0]
ingFile=sorted(filter(os.path.isfile, glob.glob(DIR+'*ingredients*.json')), key=os.path.getmtime, reverse=True)[0]
print("Product file: "+proFile)
print("Ingredients file: "+ingFile)
mergedFile='res_'+os.path.basename(proFile)
ingredients = {}
with open(ingFile,'rb') as iFile:
    for line in iFile:
        for k,v in json.loads(line).iteritems():
            ingredients[k] = v
with open(proFile,'rb') as pFile:
    products = json.load(pFile)
for p in products:
    code = p["code"]
    if code in ingredients:
        #print ingredients[code]
        r = regex.compile(r',\s*(?![^\(\)]*\))\s*')
        if len(ingredients[code])>0: # exclude case of empty list
            s = string.replace(ingredients[code],'Zutaten: ','')
            s = string.replace(s,u'In Großbuchstaben angegebene Zutaten enthalten allergene Inhaltsstoffe.','')
            s = s.rstrip('.,') 
            p["ingredients"] = filter(lambda v: v is not None, r.split(s) )
            # remove duplicate ingredients 
            seen = set()
            seen_add = seen.add
            p["ingredients"] = [x for x in p["ingredients"] if not (x in seen or seen_add(x))]
print("Output to: "+mergedFile)
with open(mergedFile,'wb') as pFile:
    json.dump(products, pFile)
