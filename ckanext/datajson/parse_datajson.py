from ckan.lib.munge import munge_title_to_name

import re

def parse_datajson_entry(datajson, package, defaults):
	package["title"] = datajson.get("title", defaults.get("Title"))
	package["notes"] = datajson.get("description", defaults.get("Notes"))

	# backwards-compatibility for files from Socrata
	if isinstance(datajson.get("keyword"), str):
		package["tags"] = [ { "name": munge_title_to_name(t) } for t in
			datajson.get("keyword").split(",") if t.strip() != ""]
	# field is provided correctly as an array...
	elif isinstance(datajson.get("keyword"), list):
		package["tags"] = [ { "name": munge_title_to_name(t) } for t in
			datajson.get("keyword") if t.strip() != ""]
	# field is not provided, use defaults specified in harvester config
	elif isinstance(defaults.get("Tags"), list):
		package["tags"] = [ { "name": t } for t in defaults.get("Tags")]

	package["groups"] = [ { "name": g } for g in 
		defaults.get("Groups", [])] # the complexity of permissions makes this useless, CKAN seems to ignore
	extra(package, "Group Name", defaults.get("Group Name")) # i.e. dataset grouping string
	extra(package, "Date Updated", datajson.get("modified"))
	extra(package, "Agency", defaults.get("Agency")) # i.e. federal department: not in data.json spec but required by the HHS metadata schema
	package["author"] = datajson.get("publisher", defaults.get("Author")) # i.e. agency within HHS
	extra(package, "author_id", defaults.get("author_id")) # i.e. URI for agency: not in data.json spec but in HHS metadata schema
	extra(package, "Bureau Code", " ".join(datajson.get("bureauCode", defaults.get("Bureau Code", []))))
	extra(package, "Program Code", " ".join(datajson.get("programCode", defaults.get("Program Code", []))))
	extra(package, "Agency Program URL", defaults.get("Agency Program URL")) # i.e. URL for agency program
	extra(package, "Contact Name", datajson.get("contactPoint", defaults.get("Contact Name"))) # not in HHS schema
	extra(package, "Contact Email", datajson.get("mbox", defaults.get("Contact Email"))) # not in HHS schema
	# "identifier" is handled by the harvester
	extra(package, "Access Level", datajson.get("accessLevel")) # not in HHS schema
	extra(package, "Access Level Comment", datajson.get("accessLevelComment")) # not in HHS schema
	extra(package, "Data Dictionary", datajson.get("dataDictionary", defaults.get("Data Dictionary")))
	# accessURL is handled with the distributions below
	# webService is handled with the distributions below
	extra(package, "Format", datajson.get("format")) # not in HHS schema
	extra(package, "License Agreement", datajson.get("license"))
	#extra(package, "License Agreement Required", ...)
	extra(package, "Geographic Scope", datajson.get("spatial"))
	extra(package, "Temporal", datajson.get("temporal")) # HHS uses Coverage Period (FY) Start/End
	extra(package, "Date Released", datajson.get("issued"))
	#extra(package, "Collection Frequency", ...) # in HHS schema but not in POD schema
	extra(package, "Publish Frequency", datajson.get("accrualPeriodicity")) # not in HHS schema but in POD schema
	extra(package, "Language", datajson.get("language")) # not in HHS schema
	extra(package, "Granularity", datajson.get("granularity")) # not in HHS schema
	extra(package, "Data Quality Met", { True: "true", False: "false" }.get(datajson.get("dataQuality"))) # not in HHS schema
	#extra(package, "Unit of Analysis", ...)
	#extra(package, "Collection Instrument", ...)
	extra(package, "Subject Area 1", datajson.get("theme", defaults.get("Subject Area 1")))
	extra(package, "Subject Area 2", defaults.get("Subject Area 2"))
	extra(package, "Subject Area 2", defaults.get("Subject Area 3"))
	extra(package, "Technical Documentation", datajson.get("references"))
	extra(package, "Size", datajson.get("size")) # not in HHS schema
	package["url"] = datajson.get("landingPage", datajson.get("webService", datajson.get("accessURL")))
	extra(package, "PrimaryITInvestmentUII", datajson.get("PrimaryITInvestmentUII")) # not in HHS schema
	extra(package, "System Of Records", datajson.get("systemOfRecords")) # not in HHS schema

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
	
def extra(package, key, value):
	if not value: return
	package.setdefault("extras", []).append({ "key": key, "value": value })
	
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
