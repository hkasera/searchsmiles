#!/var/lib/openshift/582177010c1e66dfb5000065/python/virtenv/venv/bin/python
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
