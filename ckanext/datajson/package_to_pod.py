import collections

import logging

log = logging.getLogger(__name__)

def make_datajson_entry(package):

    extras = dict([(x['key'], x['value']) for x in package['extras']])
    log.debug('package: %s', package)

    return collections.OrderedDict([
        ("title", package.get("title", None)),
        ("description", package.get("notes", None)),
        ("keyword", [t["display_name"] for t in package.get("tags", None)]),
        ("modified", package.get("metadata_modified", None)),
        ("publisher", extras.get('publisher', package.get('author', None))),
        ("organization", package.get("organization", None)),
        ('contactName', extras.get('contact_name', None)),
        ('contactEmail', extras.get('contact_email', None)),
        ("identifier", extras.get('unique_id', package.get('id', None))),
        ("accessLevel", extras.get('public_access_level', 'Public')),
        ("dataDictionary", extras.get('data_dictionary', extras.get("Data Dictionary"))),
        ("accessURL", get_primary_resource(package).get("url", None)), #TODO Is this common core?
        ("webService", get_api_resource(package).get("url", None)), #TODO Is this common core?
        ("format", get_primary_resource(package).get("format", None)), #TODO Is this common core?
        ("license", extras.get("License Agreement", None)), #TODO grab license from package?
        ('endpoint', extras.get('endpoint', None)), #TODO is this the same as the webService above?
        ("spatial", extras.get('spatial', extras.get("Geographic Scope", None))),
        ("temporal", extras.get('temporal', build_temporal(package))),
        ("issued", extras.get('release_date', extras.get("Date Released", None))), #TODO should this be release date?
        ('accrualPeriodicity', extras.get('accrual_periodicity', None)),
        ('language', extras.get('language', None)),
        ("granularity", extras.get('granularity', "/".join(x for x in [extras.get("Unit of Analysis", None), extras.get("Geographic Granularity", None)] if x != None))),
        ("dataQuality", extras.get('data_quality', True)),
        #        ("theme", [s for s in extras.get("Subject Area 1", None),extras.get("Subject Area 2", None), extras.get("Subject Area 3", None)) if s != None]),
        ("references", [s for s in [extras.get("Technical Documentation", None)] if s != None]),
        ('size', extras.get('size', None)),
        ("landingPage", extras.get('homepage_url', package["url"])), #Is this the same as homepage url?
         ('rssFeed', extras.get('rss_feed', None)),
         ('category', extras.get('category', None)),
         ('relatedDocuments', extras.get('related_documents', None)),
         ('systemOfRecords', extras.get('system_of_records', None)),
         ('systemOfRecordsNoneRelatedToThisDataset', extras.get('system_of_records_none_related_to_this_dataset', None)),
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
          ])
    ])
    
def extra(package, key, default=None):
    # Retrieves the value of an extras field.
    for extra in package["extras"]:
        if extra["key"] == key:
            return extra["value"]
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

