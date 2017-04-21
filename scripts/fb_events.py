#!/var/lib/openshift/ID/python/virtenv/venv/bin/python
import re
import requests
import json

API_BASE = "https://graph.facebook.com/v2.8/"
fields = ["about","contact_address","description","current_location",
			"fan_count","general_info,hours","is_verified","location","mission","name","phone","website"]
ACCESS_TOKEN = ""

fb_op_file = open("raw/fb_op.txt",'w')

def getUrl(pageID):
	url = API_BASE + pageID + "?fields=" + ",".join(fields)+"&access_token=" +ACCESS_TOKEN
	return url

def getEventUrl(pageID):
	url = API_BASE + pageID + "/events"+"&access_token=" +ACCESS_TOKEN
	return url

def fetchURL(pageURL):
	req = requests.get(pageURL)
	res = req.json()
	fb_op_file.write(json.dumps(res, sort_keys=True))
	fb_op_file.write("\n")


links = ["http://www.facebook.com/pages/%D7%99%D7%95%D7%96%D7%9E%D7%95%D7%AA-%D7%A7%D7%A8%D7%9F-%D7%90%D7%91%D7%A8%D7%94%D7%9D/154698021323966?ref=stream",
"https://www.facebook.com/pages/Aha-Punana-Leo/325405004197282?ref=stream",
"http://www.facebook.com/acttheatre",
"https://www.facebook.com/jewishvoiceforpeace",
"http://www.facebook.com/akidagain",
"https://facebook.com/A-Noise-Within-Theatre-Company-49514622413/",
"http://www.facebook.com/pages/A-Place-Called-Home/50218738285",
"https://www.facebook.com/AAAFTS",
"https://www.facebook.com/AAUW.National",
"http://www.facebook.com/abilityexperience",
"https://www.facebook.com/AbodeServices"]

for link in links:
	if "pages" not in link:
		i = link.index("/", link.index("facebook"))
		pageID = link[i+1:].replace("/","")
		pageURL = getUrl(pageID)
		fetchURL(pageURL)
