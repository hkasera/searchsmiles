import requests
import re
import json
import os

def checkForEvents(url):
	r = requests.get(url)
	link_re = re.compile(r'href="(.*?)"')
	events = {}
	if r.status_code == 200:
		links = link_re.findall(r.text)
		events["ngo_link"] = url
		events["event_link"] = []
		for link in links:
			if "events" in link:
				events["event_link"].append(link)
			elif "facebook.com" in link:
				events["fb_link"] = link
			elif "twitter.com" in link:
				events["tw_link"] = link
		op.write(json.dumps(events, sort_keys=True).decode('unicode-escape').encode('utf8'))
		op.write(",\n")


OPENSHIFT_LOG_DIR = os.getenv("OPENSHIFT_LOG_DIR") 
OPENSHIFT_REPO_DIR = os.getenv("OPENSHIFT_REPO_DIR") 

f = os.path.join(OPENSHIFT_REPO_DIR,'data/urls.txt')
op = open(os.path.join(OPENSHIFT_REPO_DIR,'data/events.json'),'w')
err = open(os.path.join(OPENSHIFT_LOG_DIR,'err.txt'),'w')
urls = f.readlines()
for url in urls:
	if len(url.strip()) != 0:
		try :
			checkForEvents(url.strip())
		except Exception as e:
			err.write("#############\n");
			err.write(url+"\n");
			err.write(str(e)+"\n");

f.close()
op.close()
err.close()

		