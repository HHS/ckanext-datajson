from ckan.lib.munge import munge_title_to_name

import re

from ckanext.datajson.harvester_base import DatasetHarvesterBase

def parse_datajson_entry(datajson, package, harvester_config):
	# Notes:
	# * the data.json field "identifier" is handled by the harvester

	package["title"] = datajson.get("title", package.get("title"))
	package["notes"] = datajson.get("description", package.get("notes"))
	package["author"] = datajson.get("publisher", package.get("author"))
	package["url"] = datajson.get("landingPage", datajson.get("webService", datajson.get("accessURL", package.get("url"))))

	package["groups"] = [ { "name": g } for g in 
		harvester_config["defaults"].get("Groups", [])] # the complexity of permissions makes this useless, CKAN seems to ignore

	# backwards-compatibility for files from Socrata
	if isinstance(datajson.get("keyword"), str):
		package["tags"] = [ { "name": munge_title_to_name(t) } for t in
			datajson.get("keyword").split(",") if t.strip() != ""]
	# field is provided correctly as an array...
	elif isinstance(datajson.get("keyword"), list):
		package["tags"] = [ { "name": munge_title_to_name(t) } for t in
			datajson.get("keyword") if t.strip() != ""]

	extra(package, "Group Name", datajson, "__not__in_pod__schema") # i.e. dataset grouping string from HHS schema
	extra(package, "Date Updated", datajson, "modified")
	extra(package, "Agency", datajson, "__not__in_pod__schema") # i.e. federal department: not in data.json spec but required by the HHS metadata schema
	extra(package, "author_id", datajson, "__not__in_pod__schema") # i.e. URI for agency: not in data.json spec but in HHS metadata schema
	extra(package, "Bureau Code", datajson, "bureauCode")
	extra(package, "Program Code", datajson, "programCode")
	extra(package, "Agency Program URL", datajson, "__not__in_pod__schema") # i.e. URL for agency program
	extra(package, "Contact Name", datajson, "contactPoint") # not in HHS schema
	extra(package, "Contact Email", datajson, "mbox") # not in HHS schema
	extra(package, "Access Level", datajson, "accessLevel") # not in HHS schema
	extra(package, "Access Level Comment", datajson, "accessLevelComment") # not in HHS schema
	extra(package, "Data Dictionary", datajson, "dataDictionary")
	# accessURL is handled with the distributions below
	# webService is handled with the distributions below
	extra(package, "Format", datajson, "format") # not in HHS schema
	extra(package, "License Agreement", datajson, "license")
	extra(package, "Geographic Scope", datajson, "spatial")
	extra(package, "Temporal", datajson, "temporal") # HHS uses Coverage Period (FY) Start/End
	extra(package, "Date Released", datajson, "issued")
	extra(package, "Publish Frequency", datajson, "accrualPeriodicity") # not in HHS schema but in POD schema
	extra(package, "Language", datajson, "language") # not in HHS schema
	extra(package, "Granularity", datajson, "granularity") # not in HHS schema
	extra(package, "Data Quality Met", datajson, "dataQuality") # not in HHS schema
	extra(package, "Subject Area 1", datajson, "theme")
	extra(package, "Subject Area 2", datajson, "__not__in_pod__schema")
	extra(package, "Subject Area 2", datajson, "__not__in_pod__schema")
	extra(package, "Technical Documentation", datajson, "references")
	extra(package, "PrimaryITInvestmentUII", datajson, "PrimaryITInvestmentUII") # not in HHS schema
	extra(package, "System Of Records", datajson, "systemOfRecords") # not in HHS schema

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
	
def extra(package, ckan_key, datajson, datajson_fieldname):
	value = datajson.get(datajson_fieldname)
	if not value: return
	DatasetHarvesterBase.set_extra(package, ckan_key, value)
	
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
