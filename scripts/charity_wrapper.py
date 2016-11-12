import requests
from bs4 import BeautifulSoup
import json
import re
import os

OPENSHIFT_LOG_DIR = os.getenv("OPENSHIFT_LOG_DIR") 
OPENSHIFT_REPO_DIR = os.getenv("OPENSHIFT_REPO_DIR") 

def extract(link):
	ngo = {}
	ngo['source_link'] = link
	r = requests.get(link)
	print(link)
	print(r.status_code)
	if r.status_code == 200:
		soup = BeautifulSoup(r.text, 'html.parser')	
		if soup.find(id='orgSiteLink') is not None:
			ngo['link'] = soup.find(id='orgSiteLink')['href'].strip().replace(u"\u00A0", " ").replace("'","")
		if soup.findAll("h1", { "class" : "charityname" }) is not None and len(soup.findAll("h1", { "class" : "charityname" })) != 0:
			ngo['name'] = soup.findAll("h1", { "class" : "charityname" })[0].get_text().strip().replace(u"\u00A0", " ").replace("'","")
		if soup.find(id='item25') is not None:
			mission_ele = soup.find(id="mission").next_sibling.next_sibling
			if mission_ele.find('p'):
				ngo['mission'] = mission_ele.find("p").get_text().strip().replace(u"\u00A0", " ").replace("'","")
		contact_ele = soup.findAll('h1', text = re.compile('Charity Contact Info'));
		if contact_ele is not None:
			ngo['address'] = contact_ele[0].next_sibling.next_sibling.get_text().strip().replace(u"\u00A0", " ").replace("'","")
		board_ele = soup.findAll('h1', text = re.compile('Board Leadership'));
		if board_ele is not None:
			ngo['board_leader'] = board_ele[0].next_sibling.next_sibling.get_text().strip().replace(u"\u00A0", " ").replace("'","")
		ceo_ele = soup.findAll('h1', text = re.compile('CEO'));
		if ceo_ele is not None:
			ngo['ceo'] = ceo_ele[0].next_sibling.next_sibling.get_text().strip().replace(u"\u00A0", " ").replace("'","")
		if soup.findAll("p", { "class" : "crumbs" }) is not None:
			areas = soup.findAll("p", { "class" : "crumbs" })[0].get_text().strip().replace(u"\u00A0", " ").replace("'","")
			ngo['areas'] = areas.split(":")
		if soup.findAll("h2", { "class" : "tagline" }) is not None:
			ngo['tagline'] = soup.findAll("h2", { "class" : "tagline" })[0].get_text().strip().replace(u"\u00A0", " ").replace("'","")
		
		json_file.write(json.dumps(ngo, sort_keys=True))
		json_file.write(",")
		json_file.write("\n")


links_file = os.path.join(OPENSHIFT_REPO_DIR, "links/charity.txt")
op = open(links_file,'r');
json_file_name = os.path.join(OPENSHIFT_REPO_DIR, "raw/charity_nav.txt")
json_file = open(json_file_name,'w')
error = open(os.path.join(OPENSHIFT_LOG_DIR, "charity_errors.txt"),'a')
links = op.readlines()
for link in links:
	try :
		link = link.strip().replace("&amp;","&")
		print(link)
		extract(link)
	except Exception as e:
		error.write(link)
		error.write("\n")
		error.write(str(e))


