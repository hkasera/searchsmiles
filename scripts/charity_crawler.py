import requests
import re
import os

OPENSHIFT_LOG_DIR = os.getenv("OPENSHIFT_LOG_DIR") 
OPENSHIFT_REPO_DIR = os.getenv("OPENSHIFT_REPO_DIR") 

WEBSITE_1 = "https://www.charitynavigator.org/index.cfm?bay=search.alpha"

link_re = re.compile(r'href="(.*?)"')
output_file = os.path.join(OPENSHIFT_REPO_DIR, "charity.txt")

more_links = []
op = open(output_file,'a');

def crawl(url):
	r = requests.get(url)
	responseText = r.text
	if r.status_code == 200:
		links = link_re.findall(responseText)
		for link in links:
			if "orgid" in link:
				op.write(link)
				op.write("\n")

r = requests.get(WEBSITE_1);
responseText = r.text

if r.status_code == 200:

	links = link_re.findall(responseText)

	for link in links:

		if "orgid" in link:
			op.write(link)
			op.write("\n")
		elif "search.alpha" in link and "ltr" in link:
			more_links.append(link)

for link in more_links:
	crawl(link)



