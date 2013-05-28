import ckan.plugins as p

from ckan.lib.base import BaseController, render, config
from webhelpers.html import literal
from pylons import c, request, response
import json, collections

import ckan.model
from ckan.logic.action.get import current_package_list_with_resources

class DataJsonPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IRoutes, inherit=True)
    
    def before_map(self, m):
        return m
    
    def after_map(self, m):
        m.connect('datajson', '/data.json', controller='ckanext.datajson.plugin:DataJsonController', action='index')
        return m

class DataJsonController(BaseController):

    def index(self):
        # set content type (charset required or pylons throws an error)
        response.content_type = 'application/json; charset=UTF-8'
        
        # allow caching of response (e.g. by Apache)
        del response.headers["Cache-Control"]
        del response.headers["Pragma"]
        
        # output
        data = make_json()
        return literal(json.dumps(data))

def make_json():
    # Build the data.json file.
    packages = current_package_list_with_resources( { "model": ckan.model}, {})
    return [make_entry(p) for p in packages]
    
def make_entry(package):
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
        ("temporal", temporal),
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
                {
                    "accessURL" if r["format"].lower() not in ("api", "query tool") else "webService": r["url"],
                    "format": r["format"],
                    # language
                    # size
                }
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

