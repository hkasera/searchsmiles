#!/var/lib/openshift/582177010c1e66dfb5000065/python/virtenv/venv/bin/python
import requests
import re
import os

OPENSHIFT_LOG_DIR = os.getenv("OPENSHIFT_LOG_DIR") 
OPENSHIFT_REPO_DIR = os.getenv("OPENSHIFT_REPO_DIR") 

WEBSITE_1 = "https://www.unodc.org/ngo/list.jsp"
BASE_WEBSITE_1 = "https://www.unodc.org/ngo/"
link_re = re.compile(r"href='(.*?)'")
output_file = os.path.join(OPENSHIFT_REPO_DIR, "undoc_links.txt")

op = open(output_file,'a');
r = requests.get(WEBSITE_1);
responseText = r.text
print r.status_code;
if r.status_code == 200:
	links = link_re.findall(responseText)
	for link in links:
		if "showSingleDetailed" in link:
			op.write(BASE_WEBSITE_1 + link)
			op.write("\n")