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
	# accessURL is redundant with resources
	# webService is redundant with resources
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
	package["resources"] = [ ]
	for d in datajson.get("distribution", []):
		for k in ("accessURL", "webService"):
			if d.get(k, "").strip() != "":
				r = {
					"url": d[k],
					"format": normalize_format(d.get("format", "Query Tool" if k == "webService" else "Unknown")),

					# Since we normalize the 'format' for the benefit of our Drupal site,
					# also store the MIME type as provided in the resource mimetype field.
					"mimetype": d.get("format"),
				}
				
				# Work-around for Socrata-style formats array. Pull from the value field
				# if it is set, otherwise the label.
				try:
					r["format"] = normalize_format(d["formats"][0]["label"], raise_on_unknown=True)
				except:
					pass
				try:
					r["format"] = normalize_format(d["formats"][0]["value"], raise_on_unknown=True)
				except:
					pass
				try:
					r["mimetype"] = d["formats"][0]["value"]
				except:
					pass
				
				# Name the resource the same as the normalized format, since we have
				# nothing better.
				r["name"] = r["format"]

				package["resources"].append(r)
	
def extra(package, key, value):
	if not value: return
	package.setdefault("extras", []).append({ "key": key, "value": value })
	
def normalize_format(format, raise_on_unknown=False):
	# Format should be a file extension. But sometimes Socrata outputs a MIME type.
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
