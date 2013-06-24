import collections

def make_datajson_entry(package):
    return collections.OrderedDict([
        ("title", package["title"]),
        ("description", package["notes"]),
        ("keyword", ",".join(t["display_name"] for t in package["tags"])),
        ("modified", extra(package, "Date Updated")),
        ("publisher", package["author"]),
        # person
        # mbox
        ("identifier", package["id"]),
        ("accessLevel", "Public"),
        ("dataDictionary", extra(package, "Data Dictionary")),
        ("accessURL", get_primary_resource(package).get("url", None)),
        ("webService", get_api_resource(package).get("url", None)),
        ("format", get_primary_resource(package).get("format", None)),
        ("license", extra(package, "License Agreement")),
        ("spatial", extra(package, "Geographic Scope")),
        ("temporal", build_temporal(package)),
        ("issued", extra(package, "Date Released")),
        # accrualPeriodicity (frequency of publishing, not the collection frequency)
        # language
        ("granularity", "/".join(x for x in [extra(package, "Unit of Analysis"), extra(package, "Geographic Granularity")] if x != None)),
        ("dataQuality", True),
        ("theme", extra(package, "Subject Area 1")),
        ("references", extra(package, "Technical Documentation")),
        # size
        ("landingPage", package["url"]),
        # feed
        # systemOfRecords
        ("distribution",
            [
                collections.OrderedDict([
                	("identifier", r["id"]), # NOT in POD standard, but useful for conversion to JSON-LD
                    ("accessURL" if r["format"].lower() not in ("api", "query tool") else "webService",
                    	r["url"]),
                    ("format", r["format"]),
                    # language
                    # size
                ])
                for r in package["resources"]
            ]),
    ])
    
def extra(package, key, default=None):
    # Retrieves the value of an extras field.
    for extra in package["extras"]:
        if extra["key"] == key:
            return eval(extra["value"]) # why is everything quoted??
    return default

def get_best_resource(package, acceptable_formats):
    resources = list(r for r in package["resources"] if r["format"].lower() in acceptable_formats)
    if len(resources) == 0: return { }
    resources.sort(key = lambda r : acceptable_formats.index(r["format"].lower()))
    return resources[0]

def get_primary_resource(package):
    # Return info about a "primary" resource. Select a good one.
    return get_best_resource(package, ("csv", "xls", "xml", "text", "zip", "rdf"))
    
def get_api_resource(package):
    # Return info about an API resource.
    return get_best_resource(package, ("api", "query tool"))

def build_temporal(package):
    # Build one dataset entry of the data.json file.
    temporal = ""
    if extra(package, "Coverage Period Fiscal Year Start"):
        temporal = "FY" + extra(package, "Coverage Period Fiscal Year Start")
    else:
        temporal = extra(package, "Coverage Period Start", "Unknown")
    temporal += " to "
    if extra(package, "Coverage Period Fiscal Year End"):
        temporal = "FY" + extra(package, "Coverage Period Fiscal Year End")
    else:
        temporal = extra(package, "Coverage Period End", "Unknown")
    

