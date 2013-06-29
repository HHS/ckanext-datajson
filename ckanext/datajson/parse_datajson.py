def parse_datajson_entry(datajson, package):
	package["title"] = datajson.get("title")
	package["notes"] = datajson.get("description")
	package["tags"] = [ { "name": t } for t in datajson.get("keyword", "").split(",") if t.strip() != ""]
	extra(package, "Date Updated", datajson.get("modified"))
	package["author"] = datajson.get("publisher")
	extra(package, "Contact Person", datajson.get("person")) # not in HHS schema
	extra(package, "Contact Email", datajson.get("mbox")) # not in HHS schema
	# "identifier" is handled by the harvester
	extra(package, "Access Level", datajson.get("accessLevel")) # not in HHS schema
	extra(package, "Data Dictionary", datajson.get("dataDictionary"))
	# accessURL is redundant with resources
	# webService is redundant with resources
	extra(package, "Format", datajson.get("format")) # not in HHS schema
	extra(package, "License Agreement", datajson.get("license"))
	extra(package, "Geographic Scope", datajson.get("spatial"))
	extra(package, "Temporal", datajson.get("temporal")) # HHS uses Coverage Period Start/End
	extra(package, "Date Released", datajson.get("issued"))
	extra(package, "Publish Frequency", datajson.get("accrualPeriodicity")) # not in HHS schema
	extra(package, "Language", datajson.get("language")) # not in HHS schema
	extra(package, "Granularity", datajson.get("granularity")) # not in HHS schema
	extra(package, "Data Quality Met", datajson.get("dataQuality")) # not in HHS schema
	extra(package, "Subject Area 1", datajson.get("theme"))
	extra(package, "Technical Documentation", datajson.get("references"))
	extra(package, "Size", datajson.get("size")) # not in HHS schema
	package["url"] = datajson.get("landingPage")
	extra(package, "Feed", datajson.get("feed")) # not in HHS schema
	extra(package, "System Of Records", datajson.get("systemOfRecords")) # not in HHS schema
	package["resources"] = [ ]
	for d in datajson.get("distribution", []):
		for k in ("accessURL", "webService"):
			if d.get(k, "").strip() != "":
				r = {
					"url": d[k],
					"format": d.get("format", "Query Tool" if k == "webService" else "Unknown"),
					"name": d.get("format"),
				}
				extra(r, "Language", d.get("language"))
				extra(r, "Size", d.get("size"))
				package["resources"].append(r)
	
def extra(package, key, value):
	if not value: return
	package.setdefault("extras", []).append({ "key": key, "value": value })
	
