try:
    from collections import OrderedDict # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

def make_datajson_entry(package):
    return OrderedDict([
        ("title", package["title"]),
        ("description", package["notes"]),
        ("keyword", [t["display_name"] for t in package["tags"]]),
        ("modified", extra(package, "Date Updated")),
        ("publisher", package["author"]),
        # person
        # mbox
        ("identifier", package["id"]),
        ("accessLevel", "public"),
        ("dataDictionary", extra(package, "Data Dictionary")),
        ("accessURL", get_primary_resource(package).get("url", None)),
        ("webService", get_api_resource(package).get("url", None)),
        ("format", [ extension_to_mime_type(get_primary_resource(package).get("format", None)) ]),
        ("license", extra(package, "License Agreement")),
        ("spatial", extra(package, "Geographic Scope")),
        ("temporal", build_temporal(package)),
        ("issued", extra(package, "Date Released")),
        # accrualPeriodicity (frequency of publishing, not the collection frequency)
        # language
        ("granularity", "/".join(x for x in [extra(package, "Unit of Analysis"), extra(package, "Geographic Granularity")] if x != None)),
        ("dataQuality", True),
        ("theme", [s for s in (extra(package, "Subject Area 1"), extra(package, "Subject Area 2"), extra(package, "Subject Area 3")) if s != None]),
        ("references", [s for s in [extra(package, "Technical Documentation")] if s != None]),
        ("landingPage", package["url"]),
        # feed
        # systemOfRecords
        ("distribution",
            [
                OrderedDict([
                   ("identifier", r["id"]), # NOT in POD standard, but useful for conversion to JSON-LD
                   ("accessURL", r["url"]),
                   ("format", extension_to_mime_type(r["format"])),
                ])
                for r in package["resources"]
                if r["format"].lower() not in ("api", "query tool", "widget")
            ]),
    ])
    
def extra(package, key, default=None):
    # Retrieves the value of an extras field.
    for extra in package["extras"]:
        if extra["key"] == key:
            return extra["value"]
    return default

def get_best_resource(package, acceptable_formats, unacceptable_formats=None):
    resources = list(r for r in package["resources"] if r["format"].lower() in acceptable_formats)
    if len(resources) == 0:
        if unacceptable_formats:
            # try at least any resource that's not unacceptable
            resources = list(r for r in package["resources"] if r["format"].lower() not in unacceptable_formats)
        if len(resources) == 0:
            # there is no acceptable resource to show
            return { }
    else:
        resources.sort(key = lambda r : acceptable_formats.index(r["format"].lower()))
    return resources[0]

def get_primary_resource(package):
    # Return info about a "primary" resource. Select a good one.
    return get_best_resource(package, ("csv", "xls", "xml", "text", "zip", "rdf"), ("api", "query tool", "widget"))
    
def get_api_resource(package):
    # Return info about an API resource.
    return get_best_resource(package, ("api", "query tool"))

def build_temporal(package):
    # Build one dataset entry of the data.json file.
    temporal = ""
    if extra(package, "Coverage Period Fiscal Year Start"):
        temporal = "FY" + extra(package, "Coverage Period Fiscal Year Start").replace(" ", "T").replace("T00:00:00", "")
    else:
        temporal = extra(package, "Coverage Period Start", "Unknown").replace(" ", "T").replace("T00:00:00", "")
    temporal += "/"
    if extra(package, "Coverage Period Fiscal Year End"):
        temporal += "FY" + extra(package, "Coverage Period Fiscal Year End").replace(" ", "T").replace("T00:00:00", "")
    else:
        temporal += extra(package, "Coverage Period End", "Unknown").replace(" ", "T").replace("T00:00:00", "")
    if temporal == "Unknown/Unknown": return None
    return temporal

def extension_to_mime_type(file_ext):
    if file_ext is None: return None
    ext = {
        "csv": "text/csv",
        "xls": "application/vnd.ms-excel",
        "xml": "application/xml",
        "rdf": "application/rdf+xml",
        "json": "application/json",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "text": "text/plain",
        "feed": "application/rss+xml",
    }
    return ext.get(file_ext.lower(), "application/unknown")
    
