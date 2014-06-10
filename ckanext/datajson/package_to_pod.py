import re
import sys
import validator
from validator import URL_REGEX
from validator import email_validator
from pprint import pprint

try:
    from collections import OrderedDict # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

def get_facet_fields():
    # Return fields that we'd like to add to default CKAN faceting. This really has
    # nothing to do with exporting data.json but it's probably a common consideration.
    facets = OrderedDict()
    facets["Agency"] = "Publishers" # using "author" produces weird results because the Solr schema indexes it as "text" rather than "string"
    facets["SubjectArea1"] = "Subjects" # search facets remove spaces from field names
    return facets

def make_datajson_entry(package, plugin):
    # keywords
    keywords = [t["display_name"] for t in package["tags"]]
    if len(keywords) == 0 and plugin.default_keywords is not None:
        keywords = re.split("\s*,\s*", plugin.default_keywords)

    # Make a default program code value when a bureau code is none. The
    # departmental component of a bureau code plus ":000" means
    # "Primary Program Not Available".
    defaultProgramCode = None
    if extra(package, "Bureau Code"):
        defaultProgramCode = [bcode.split(":")[0] + ":000" for bcode in extra(package, "Bureau Code").split(" ")]
 
#    f1=open('/tmp/testfile', 'a+')

    if package["url"] != None and not URL_REGEX.match(package["url"]):
        package["url"] = None
    for r in package["resources"]:
        if r["url"] != None and not URL_REGEX.match(r["url"]):
            r["url"] = None
        if r["mimetype"] != None:
            r["mimetype"] = re.sub(r"[,\s].*", "", r["mimetype"])
        # r.get doesn't get this right, so logic it here
        if r["mimetype"] == None:
            r["mimetype"] = extension_to_mime_type(r["format"])


    # The 'modified' field needs to be populated somehow, try all the date
    # fields we can think of.
    modified = extra(package, "Date Updated", datatype="iso8601", default=extra(package, "Date Released", datatype="iso8601", default=extra(package, "harvest_last_updated", datatype="iso8601", default=extra(package, "Coverage Period Start", datatype="iso8601", default=package["revision_timestamp"]))))

    access_url = get_primary_resource(package).get("url", package['url'])
    mbox = extra(package, "Contact Email", default=plugin.default_mbox)
    if not email_validator(mbox):
        mbox = plugin.default_mbox

    language = extra(package, "Language")
    if isinstance(language, basestring):
        language = [language]
    if package["notes"] == None or package["notes"] == "":
        package["notes"] = access_url
#    print >>f1, "language for "+package['id']
#    print >>f1, language
#        if orig_url != None:
#            print >>f1, "Was "+orig_url
#    foo = get_primary_resource(package)
#    pprint(foo, f1)
#        print >>f1, foo.get("url", None)
#        print >>f1, "\n"
#    pprint(package, f1)
#    print >>f1, "\n"

    # form the return value as an ordered list of fields which is nice for doing diffs of output
    ret = [
        ("title", package["title"]),
        ("description", package["notes"]),
        ("keyword", keywords),
        ("modified", modified),
        ("publisher", package["author"]),
        ("bureauCode", extra(package, "Bureau Code").split(" ") if extra(package, "Bureau Code") else None),
        ("programCode", extra(package, "Program Code").split(" ") if extra(package, "Program Code") else defaultProgramCode),
        ("contactPoint", extra(package, "Contact Name", default=plugin.default_contactpoint)),
        ("mbox", mbox),
        ("identifier", package["id"]),
        ("accessLevel", extra(package, "Access Level", default="public")),
        ("accessLevelComment", extra(package, "Access Level Comment")),
        ("dataDictionary", extra(package, "Data Dictionary")),
        ("accessURL", access_url),
        ("webService", get_api_resource(package).get("url", None)),
        ("format", extension_to_mime_type(get_primary_resource(package).get("format", None)) ),
        ("license", extra(package, "License Agreement")),
        ("spatial", extra(package, "Geographic Scope")),
        ("temporal", build_temporal(package)),
        ("issued", extra(package, "Date Released", datatype="iso8601")),
        ("accrualPeriodicity", extra(package, "Publish Frequency")),
        ("language", language),
        ("PrimaryITInvestmentUII", extra(package, "PrimaryITInvestmentUII")),
        ("dataQuality", extra(package, "Data Quality Met", default="true") == "true"),
        ("theme", [s for s in (extra(package, "Subject Area 1"), extra(package, "Subject Area 2"), extra(package, "Subject Area 3")) if s != None]),
        ("references", [s for s in extra(package, "Technical Documentation", default="").split(" ") if s != ""]),
        ("landingPage", package["url"]),
        ("systemOfRecords", extra(package, "System Of Records")),
        ("distribution",
            [
                OrderedDict([
                   ("identifier", r["id"]), # NOT in POD standard, but useful for conversion to JSON-LD
                   ("accessURL", r["url"]),
                   ("format", r.get("mimetype", extension_to_mime_type(r["format"]))),
                ])
                for r in package["resources"]
                if r["format"].lower() not in ("api", "query tool", "widget") and r["url"] != None
            ]),
    ]

    # Special case to help validation.
    if extra(package, "Catalog Type") == "State Catalog":
        ret.append( ("_is_federal_dataset", False) )

    # GSA doesn't like null values and empty lists so remove those now.
    ret = [(k, v) for (k, v) in ret if v is not None and (not isinstance(v, list) or len(v) > 0)]

    # And return it as an OrderedDict because we need dict output in JSON
    # and we want to have the output be stable which is helpful for debugging (e.g. with diff).
    return OrderedDict(ret)
    
def extra(package, key, default=None, datatype=None, raise_if_missing=False):
    # Retrieves the value of an extras field.
    for extra in package["extras"]:
        if extra["key"] == key:
            v = extra["value"]
            if key == "Access Level" and v == "Public":
                v = "public"
            if key == "Data Dictionary" and " " in v:
                return default

            if datatype == "iso8601":
                # Hack: If this value is a date, convert Drupal style dates to ISO 8601
                # dates by replacing the space date/time separator with a T. Also if it
                # looks like a plain date (midnight time), remove the time component.
                v = v.replace(" ", "T")
                v = v.replace("T00:00:00", "")

            return v
    if raise_if_missing: raise ValueError("Missing value for %s.", key)
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

    # If this came from a harvested data.json file, we marked the resource
    # that came from the top-level accessURL as 'is_primary_distribution'.
    for r in package["resources"]:
        if r.get("is_primary_distribution") == 'true':
            return r

    # Otherwise fall back to a resource by prefering certain formats over others.
    return get_best_resource(package, ("csv", "xls", "xml", "text", "zip", "rdf"), ("api", "query tool", "widget"))
    
def get_api_resource(package):
    # Return info about an API resource.
    return get_best_resource(package, ("api",))

def build_temporal(package):
    # Build one dataset entry of the data.json file.
    try:
        # we ask extra() to raise if either the start or end date is missing since we can't
        # form a valid value in that case
        return \
              extra(package, "Coverage Period Start", datatype="iso8601", raise_if_missing=True) \
            + "/" \
            + extra(package, "Coverage Period End", datatype="iso8601", raise_if_missing=True)
    except ValueError:
        return None

def extension_to_mime_type(file_ext):
#    if file_ext is None: return None
#    if file_ext is "Other": return None
    if file_ext is None: return "application/unknown"
    if file_ext is "Other": return "application/unknown"
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
    
