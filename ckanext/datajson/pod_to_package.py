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
	package["groups"] = [ { "name": g } for g in 
		defaults.get("Groups", [])] # the complexity of permissions makes this useless, CKAN seems to ignore
	package["organization"] = datajson.get("organization", defaults.get("Organization"))
	extra(package, "Group Name", defaults.get("Group Name")) # i.e. dataset grouping string
	extra(package, "Date Updated", datajson.get("modified"))
	extra(package, "Agency", defaults.get("Agency")) # i.e. federal department
	package["publisher"] = datajson.get("publisher", defaults.get("Author")) # i.e. agency within HHS
	extra(package, "author_id", defaults.get("author_id")) # i.e. URI for agency
	extra(package, "Agency Program URL", defaults.get("Agency Program URL")) # i.e. URL for agency program
	extra(package, "Contact Person", datajson.get("person")) # not in HHS schema
	extra(package, "Contact Email", datajson.get("mbox")) # not in HHS schema
	# "identifier" is handled by the harvester
	extra(package, "Access Level", datajson.get("accessLevel")) # not in HHS schema
	extra(package, "Data Dictionary", datajson.get("dataDictionary", defaults.get("Data Dictionary")))
	# accessURL is redundant with resources
	# webService is redundant with resources
	extra(package, "Format", datajson.get("format")) # not in HHS schema
	extra(package, "License Agreement", datajson.get("license"))
	#extra(package, "License Agreement Required", ...)
	extra(package, "Geographic Scope", datajson.get("spatial"))
	extra(package, "Temporal", datajson.get("temporal")) # HHS uses Coverage Period (FY) Start/End
	extra(package, "Date Released", datajson.get("issued"))
	#extra(package, "Collection Frequency", ...)
	extra(package, "Publish Frequency", datajson.get("accrualPeriodicity")) # not in HHS schema
	extra(package, "Language", datajson.get("language")) # not in HHS schema
	extra(package, "Granularity", datajson.get("granularity")) # not in HHS schema
	extra(package, "Data Quality Met", datajson.get("dataQuality")) # not in HHS schema
	#extra(package, "Unit of Analysis", ...)
	#extra(package, "Collection Instrument", ...)
	extra(package, "Subject Area 1", datajson.get("theme", defaults.get("Subject Area 1")))
	extra(package, "Subject Area 2", defaults.get("Subject Area 2"))
	extra(package, "Subject Area 2", defaults.get("Subject Area 3"))
	extra(package, "Technical Documentation", datajson.get("references"))
	extra(package, "Size", datajson.get("size")) # not in HHS schema
	package["url"] = datajson.get("landingPage", datajson.get("webService", datajson.get("accessURL")))
	extra(package, "Feed", datajson.get("feed")) # not in HHS schema
	extra(package, "System Of Records", datajson.get("systemOfRecords")) # not in HHS schema
	package["resources"] = [ ]
	for d in datajson.get("distribution", []):
		for k in ("accessURL", "webService"):
			if d.get(k, "").strip() != "":
				r = {
					"url": d[k],
					"format": normalize_format(d.get("format", "Query Tool" if k == "webService" else "Unknown")),
				}
				extra(r, "Language", d.get("language"))
				extra(r, "Size", d.get("size"))
				
				# work-around for Socrata-style formats array
				try:
					r["format"] = normalize_format(d["formats"][0]["label"])
				except:
					pass
				
				r["name"] = r["format"]
				
				package["resources"].append(r)
	
def extra(package, key, value):
	if not value: return
	package.setdefault("extras", []).append({ "key": key, "value": value })
	
def normalize_format(format):
	# Format should be a file extension. But sometimes Socrata outputs a MIME type.
	format = format.lower()
	m = re.match(r"((application|text)/(\S+))(; charset=.*)?", format)
	if m:
		if m.group(1) == "text/plain": return "Text"
		if m.group(1) == "application/zip": return "ZIP"
		if m.group(1) == "application/vnd.ms-excel": return "XLS"
		if m.group(1) == "application/x-msaccess": return "Access"
		return "Other"
	if format == "text": return "Text"
	return format.upper() # hope it's one of our formats by converting to upprecase
