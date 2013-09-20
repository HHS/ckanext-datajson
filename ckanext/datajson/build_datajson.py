import collections

import logging

log = logging.getLogger(__name__)

#TODO this file is pretty sloppy, needs cleanup and redundancies removed

def make_datajson_entry(package):

    #extras is a list of dicts [{},{}, {}]. For each dict, extract the key, value entries into a new dict
    extras = dict([(x['key'], x['value']) for x in package['extras']])

    retlist = [
        ("title", package.get("title", None)),
        ("description", package.get("notes", None)),
        ("keyword", [t["display_name"] for t in package.get("tags", None)]),
        ("modified", package.get("metadata_modified", None)),
        ("publisher", extras.get('publisher', package.get('author', None))),
        ("organization", package.get("organization", None).get('title', 'None')),
        ('contactPoint', extras.get('contact_name', None)),
        ('mbox', extras.get('contact_email', None)),
        ("identifier", extras.get('unique_id', package.get('id', None))),
        ("accessLevel", extras.get('public_access_level', 'public')),
        ("dataDictionary", extras.get('data_dictionary', extras.get("Data Dictionary"))),
        ("bureauCode", extras.get("bureau_code", None)),
        ("programCode", extras.get("program_code", None)),
        ("accessLevelComment", extras.get("access_level_comment", None)),
        ("accessURL", get_primary_resource(package).get("url", None)),
        ("webService", get_api_resource(package).get("endpoint", None)),
        ("format", get_primary_resource(package).get("format", None)),
        ("license", extras.get("License Agreement", package['license_title'])),
        ("spatial", extras.get('spatial', extras.get("Geographic Scope", None))),
        ("temporal", extras.get('temporal', build_temporal(package))),
        ("issued", extras.get('release_date', extras.get("Date Released", None))),
        ('accrualPeriodicity', extras.get('accrual_periodicity', None)),
        ('language', extras.get('language', None)),
        ("granularity", extras.get('granularity', "/".join(x for x in [extras.get("Unit of Analysis", None), extras.get("Geographic Granularity", None)] if x != None))),
        ("dataQuality", extras.get('data_quality', True)),
        ("theme", [s for s in extras.get("Subject Area 1", None), extras.get("Subject Area 2", None),extras.get("Subject Area 3", None) if s != None]),
        ("references", [s for s in [extras.get("Technical Documentation", None)] if s != None]),
        ('size', extras.get('size', None)),
        ("landingPage", extras.get('homepage_url', package["url"])),
         ('rssFeed', extras.get('rss_feed', None)),
         ('category', extras.get('category', None)),
         ('relatedDocuments', extras.get('related_documents', None)),
         ('systemOfRecords', extras.get('system_of_records', None)),
         ('systemOfRecordsNoneRelatedToThisDataset', extras.get('system_of_records_none_related_to_this_dataset', None)),
         ("distribution",
          #TODO distribution should hide any key/value pairs where value is "" or None (e.g. format)
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
         ])]

    #TODO this is a lazy hack to make sure we don't have redundant fields when the free form key/value pairs are added
    extras_to_filter_out = ['publisher', 'contact_name','contact_email', 'unique_id', 'public_access_level',
                            'data_dictionary', 'bureau_code', 'program_code', 'access_level_comment', 'license_title',
                            'spatial', 'temporal', 'release_date', 'accrual_periodicity', 'language', 'granularity',
                            'data_quality', 'size', 'homepage_url', 'rss_feed', 'category', 'related_documents',
                            'system_of_records', 'system_of_records_none_related_to_this_dataset']

    #Append any free extras (key/value pairs) that aren't part of common core but have been associated with the dataset
    #TODO really hackey, short on time, had to hardcode a lot of the names to remove. there's much better ways, maybe
    #generate a list of keys to ignore by calling a specific function to get the extras
    retlist_keys = [x for x,y in retlist]
    extras_keys = set(extras.keys()) - set(extras_to_filter_out)

    log.debug('retlist_keys %s', extras_keys)

    for key in extras_keys:
        convertedKey = underscore_to_camelcase(key)
        if convertedKey not in retlist_keys:
            retlist.append((convertedKey, extras[key]))

    # Remove entries where value is None, "", or empty list []
    striped_retlist = [(x, y) for x,y in retlist if y != None and y != "" and y != []]

    return collections.OrderedDict(striped_retlist)

    
def extra(package, key, default=None):
    # Retrieves the value of an extras field.
    for extra in package["extras"]:
        if extra["key"] == key:
            return extra["value"]
    return default

def underscore_to_camelcase(value):
    """
    Convert underscored strings to camel case, e.g. one_two_three to oneTwoThree
    """
    def camelcase():
        yield unicode.lower
        while True:
            yield unicode.capitalize
    c = camelcase()
    return "".join(c.next()(x) if x else '_' for x in value.split("_"))

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

