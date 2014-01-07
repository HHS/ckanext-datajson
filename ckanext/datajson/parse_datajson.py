from ckan.lib.munge import munge_title_to_name

import re

def parse_datajson_entry(datajson, package, harvester_config):
	# Notes:
	# * the data.json field "identifier" is handled by the harvester

	package["title"] = get_field_value(datajson, "title", harvester_config, "Title")
	package["notes"] = get_field_value(datajson, "description", harvester_config, "Notes")
	package["author"] = get_field_value(datajson, "publisher", harvester_config, "Author") # i.e. agency within HHS
	package["url"] = datajson.get("landingPage", datajson.get("webService", datajson.get("accessURL")))

	package["groups"] = [ { "name": g } for g in 
		harvester_config["defaults"].get("Groups", [])] # the complexity of permissions makes this useless, CKAN seems to ignore

	# tags specified in the config overrides
	if isinstance(harvester_config["overrides"].get("Tags"), list):
		package["tags"] = [ { "name": t } for t in harvester_config["overrides"].get("Tags")]
	# backwards-compatibility for files from Socrata
	elif isinstance(datajson.get("keyword"), str):
		package["tags"] = [ { "name": munge_title_to_name(t) } for t in
			datajson.get("keyword").split(",") if t.strip() != ""]
	# field is provided correctly as an array...
	elif isinstance(datajson.get("keyword"), list):
		package["tags"] = [ { "name": munge_title_to_name(t) } for t in
			datajson.get("keyword") if t.strip() != ""]
	# field is not provided, use defaults specified in harvester config
	elif isinstance(harvester_config["defaults"].get("Tags"), list):
		package["tags"] = [ { "name": t } for t in harvester_config["defaults"].get("Tags")]

	extra(package, "Group Name", datajson, "__not__in_pod__schema", harvester_config, "Group Name") # i.e. dataset grouping string from HHS schema
	extra(package, "Date Updated", datajson, "modified", harvester_config, "Date Updated")
	extra(package, "Agency", datajson, "__not__in_pod__schema", harvester_config, "Agency") # i.e. federal department: not in data.json spec but required by the HHS metadata schema
	extra(package, "author_id", datajson, "__not__in_pod__schema", harvester_config, "author_id") # i.e. URI for agency: not in data.json spec but in HHS metadata schema
	extra(package, "Bureau Code", datajson, "bureauCode", harvester_config, "Bureau Code")
	extra(package, "Program Code", datajson, "programCode", harvester_config, "Program Code")
	extra(package, "Agency Program URL", datajson, "__not__in_pod__schema", harvester_config, "Agency Program URL") # i.e. URL for agency program
	extra(package, "Contact Name", datajson, "contactPoint", harvester_config, "Contact Name") # not in HHS schema
	extra(package, "Contact Email", datajson, "mbox", harvester_config, "Contact Email") # not in HHS schema
	extra(package, "Access Level", datajson, "accessLevel", harvester_config, "Access Level") # not in HHS schema
	extra(package, "Access Level Comment", datajson, "accessLevelComment", harvester_config, "Access Level Comment") # not in HHS schema
	extra(package, "Data Dictionary", datajson, "dataDictionary", harvester_config, "Data Dictionary")
	# accessURL is handled with the distributions below
	# webService is handled with the distributions below
	extra(package, "Format", datajson, "format", harvester_config, "Format") # not in HHS schema
	extra(package, "License Agreement", datajson, "license", harvester_config, "License")
	extra(package, "Geographic Scope", datajson, "spatial", harvester_config, "Geographic Scope")
	extra(package, "Temporal", datajson, "temporal", harvester_config, "Temporal") # HHS uses Coverage Period (FY) Start/End
	extra(package, "Date Released", datajson, "issued", harvester_config, "Date Released")
	extra(package, "Publish Frequency", datajson, "accrualPeriodicity", harvester_config, "Publish Frequency") # not in HHS schema but in POD schema
	extra(package, "Language", datajson, "language", harvester_config, "Language") # not in HHS schema
	extra(package, "Granularity", datajson, "granularity", harvester_config, "Granularity") # not in HHS schema
	extra(package, "Data Quality Met", datajson, "dataQuality", harvester_config, "Data Quality Met") # not in HHS schema
	extra(package, "Subject Area 1", datajson, "theme", harvester_config, "Subject Area 1")
	extra(package, "Subject Area 2", datajson, "__not__in_pod__schema", harvester_config, "Subject Area 2")
	extra(package, "Subject Area 2", datajson, "__not__in_pod__schema", harvester_config, "Subject Area 3")
	extra(package, "Technical Documentation", datajson, "references", harvester_config, "Technical Documentation")
	extra(package, "PrimaryITInvestmentUII", datajson, "PrimaryITInvestmentUII", harvester_config, "PrimaryITInvestmentUII") # not in HHS schema
	extra(package, "System Of Records", datajson, "systemOfRecords", harvester_config, "System Of Records") # not in HHS schema

	# In HHS schema but not in POD schema:
	# License Agreement Required, Collection Frequency, Unit of Analysis, Collection Instrument

	# Add resources.

	package["resources"] = [ ]

	def add_resource(accessURL, format, is_primary=False, socrata_formats=None):
		# skip if this has an empty accessURL
		if accessURL is None or accessURL.strip() == "": return

		# form the resource
		r = {
			"url": accessURL,

			# Store the format normalized to a file-extension-like string.
			# e.g. turn text/csv into CSV
			"format": normalize_format(format),

			# Also store the MIME type as given in the data.json file.
			"mimetype": format,
		}

		# Remember which resource came from the top-level accessURL so we can round-trip that.
		if is_primary:
			r["is_primary_distribution"] = "true"
		
		# Work-around for Socrata-style formats array. Pull from the value field
		# if it is set, otherwise the label.
		if isinstance(socrata_formats, list):
			try:
				r["format"] = normalize_format(socrata_formats[0]["label"], raise_on_unknown=True)
			except:
				pass
			try:
				r["format"] = normalize_format(socrata_formats[0]["value"], raise_on_unknown=True)
			except:
				pass
			try:
				r["mimetype"] = socrata_formats[0]["value"]
			except:
				pass
		
		# Name the resource the same as the normalized format, since we have
		# nothing better.
		r["name"] = r["format"]

		package["resources"].append(r)

	# Use the top-level accessURL and format fields only if there are no distributions, because
	# otherwise the accessURL and format should be repeated in the distributions and acts as
	# the primary distribution, whatever that might mean.
	if not isinstance(datajson.get("distribution"), list) or len(datajson.get("distribution")) == 0:
		add_resource(datajson.get("accessURL"), datajson.get("format"), is_primary=True)

	# Include the webService URL as an API resource.
	if isinstance(datajson.get("webService"), (str, unicode)):
		add_resource(datajson.get("webService"), "API")

	# ...And the distributions.
	if isinstance(datajson.get("distribution"), list):
		for d in datajson.get("distribution"):
			add_resource(d.get("accessURL"), d.get("format"), socrata_formats=d.get("formats"), is_primary=(d.get("accessURL")==datajson.get("accessURL")))
	
def get_field_value(datajson, datajson_fieldname, harvester_config, ckan_fieldname):
	# Return the value from the config's overrides key, or else the value from
	# the data.json file, or else the value from the config's defaults key.
	return harvester_config["overrides"].get(ckan_fieldname,
		      datajson.get(datajson_fieldname,
			     harvester_config["defaults"].get(ckan_fieldname)))

def set_extra(package, key, value):
	if not value: return
	package.setdefault("extras", []).append({ "key": key, "value": value })

def extra(package, ckan_key, datajson, datajson_fieldname, harvester_config, ckan_fieldname):
	value = get_field_value(datajson, datajson_fieldname, harvester_config, ckan_fieldname)
	if isinstance(value, list): value = " ".join(value) # for bureauCode, programCode, references
	if value in (True, False): value = str(value).lower() # for dataQuality which is a boolean field, turn into "true" and "false"
	set_extra(package, ckan_key, value)
	
def normalize_format(format, raise_on_unknown=False):
	# Format should be a file extension. But sometimes Socrata outputs a MIME type.
	if format is None:
		if raise_on_unknown: raise ValueError()
		return "Unknown"
	format = format.lower()
	m = re.match(r"((application|text)/(\S+))(; charset=.*)?", format)
	if m:
		if m.group(1) == "text/plain": return "Text"
		if m.group(1) == "application/zip": return "ZIP"
		if m.group(1) == "application/vnd.ms-excel": return "XLS"
		if m.group(1) == "application/x-msaccess": return "Access"
		if raise_on_unknown: raise ValueError() # caught & ignored by caller
		return "Other"
	if format == "text": return "Text"
	if raise_on_unknown and "?" in format: raise ValueError() # weird value we should try to filter out; exception is caught & ignored by caller
	return format.upper() # hope it's one of our formats by converting to upprecase
