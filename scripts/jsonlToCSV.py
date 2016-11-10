import json
import csv

jsonlFile =  open('data/charity_dump.jsonl')
jsonFile = open('data/charity_dump.json', 'w')
ngoJSON = jsonlFile.readlines()
ngoObj = []
for ngo in ngoJSON:
	jsonFile.write(ngo)
	jsonFile.write(",")
	
jsonFile.close()
