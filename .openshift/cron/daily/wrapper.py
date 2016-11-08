#!/var/lib/openshift/582177010c1e66dfb5000065/python/virtenv/venv/bin/python
import requests
from bs4 import BeautifulSoup
import json
import re
import os


def extract(link):
	ngo = {}
	ngo['source_link'] = link
	r = requests.get(link)
	#print r.status_code
	if r.status_code == 200:
		soup = BeautifulSoup(r.text, 'html.parser')	
		if soup.find('td', text=re.compile('Website:')) is not None:
			ngo['link'] = soup.find('td', text=re.compile('Website:')).next_sibling.next_sibling.get_text().strip().replace(u"\u00A0", " ").replace("'","")
		if soup.find('td', text=re.compile('Address:')) is not None:
			ngo['address'] = soup.find('td', text=re.compile('Address:')).next_sibling.next_sibling.get_text().strip().replace(u"\u00A0", " ").replace("'","")
		if soup.find('td', text=re.compile('Short description:')) is not None:
			ngo['about'] = soup.find('td', text=re.compile('Short description:')).next_sibling.next_sibling.get_text().strip().replace(u"\u00A0", " ").replace("'","")
		if soup.find('td', text=re.compile('Type:')) is not None:
			ngo['type'] = soup.find('td', text=re.compile('Type:')).next_sibling.next_sibling.get_text().strip().replace(u"\u00A0", " ").replace("'","")
		if soup.find('td', text=re.compile('Country:')) is not None:
			ngo['country'] = soup.find('td', text=re.compile('Country:')).next_sibling.next_sibling.get_text().strip().replace(u"\u00A0", " ").replace("'","")
		if soup.find('td', text=re.compile('Region:')) is not None:
			ngo['region'] = soup.find('td', text=re.compile('Region:')).next_sibling.next_sibling.get_text().strip().replace(u"\u00A0", " ").replace("'","")
		if soup.find('td', text=re.compile('Areas:')) is not None:
			areas_ele = soup.find('td', text=re.compile('Areas:')).next_sibling.next_sibling
			areas_li_ele = areas_ele.find('ul').findAll('li')
			ngo['areas'] = []
			for area_li in areas_li_ele:
				#print area_li
				ngo['areas'].append(area_li.get_text().strip().replace(u"\u00A0", " ").replace("'",""))	
		if soup.find("h2") is not None:
			ngo['name'] = soup.find("h2").get_text().strip().replace(u"\u00A0", " ").replace("'","")
		print ngo
		json_file.write(json.dumps(ngo, sort_keys=True))
		json_file.write("\n")

OPENSHIFT_LOG_DIR = os.getenv("OPENSHIFT_LOG_DIR") 
OPENSHIFT_REPO_DIR = os.getenv("OPENSHIFT_REPO_DIR") 

links_file = os.path.join(OPENSHIFT_REPO_DIR, "links/undoc.txt")
op = open(links_file,'r');
json_file_name = os.path.join(OPENSHIFT_REPO_DIR, "data/undoc_dump.jsonl")
json_file = open(json_file_name,'a')
error = open(os.path.join(OPENSHIFT_LOG_DIR, "undoc_errors.txt"),'a')
links = op.readlines()
for link in links:
	try :
		print(link)
		extract(link.strip())
	except Exception as e:
		error.write(link)
		error.write("\n")
		error.write(str(e))


