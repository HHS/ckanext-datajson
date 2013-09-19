import simplejson as json
import urllib2

response = urllib2.urlopen('http://54.225.249.14/data.json')
data = json.load(response)

# First we build a list of all of the organizations represented in the file
def build_organization_list():
	organizations = []
	for dataset in data:
		organizations.append(dataset['organization']['name'])
	return list(set(organizations))

organizations = build_organization_list()

# We'll build out the JSON file for each organization
def build_organization_json():
	for organization in organizations:
		org_json = []
		for dataset in data:
			if organization in dataset['organization']['name']:
				org_name = dataset['organization']['name']
				org_json.append(dataset)
				with open('/usr/lib/ckan/datajson/ckanext/datajson/organizations/' + org_name + '.json', 'w') as outfile:
					json.dump(org_json, outfile)

build_organization_json()
