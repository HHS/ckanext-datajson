try:
    from collections import OrderedDict # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict


import logging, string

log = logging.getLogger(__name__)

#TODO this file is pretty sloppy, needs cleanup and redundancies removed

def make_datajson_entry(package):

    #extras is a list of dicts [{},{}, {}]. For each dict, extract the key, value entries into a new dict
    extras = dict([(x['key'], x['value']) for x in package['extras']])

    retlist = []

    try:
        retlist = [
        ("title", package["title"]), #required
        ("description", package["notes"]), #required
        ("keyword", [t["display_name"] for t in package["tags"]]),#required
        ("modified", package["metadata_modified"]), #required
        ("publisher", extras.get('publisher', package['author'])), #required
        ('contactPoint', extras['contact_name']), #required
        ('mbox', extras['contact_email']), #required
        ("identifier", extras['unique_id']), #required
        ("accessLevel", extras['public_access_level']), #required
        ("dataDictionary", extras.get('data_dictionary', extras.get("Data Dictionary"))),
        ("bureauCode", extras.get("bureau_code", None)),
        ("programCode", extras.get("program_code", None)),
        ("accessLevelComment", extras.get("access_level_comment", None)),
#DWC: why is this here? should be under distribution          ("accessURL", get_primary_resource(package).get("url", None)),
        ("webService", get_api_resource(package).get("endpoint", None)),
#DWC: why is this here? should be under distribution        ("format", get_primary_resource(package).get("format", None)),
        ("license", extras.get("License Agreement", package['license_title'])),
        ("spatial", extras.get('spatial', extras.get("Geographic Scope", None))),
        ("temporal", extras.get('temporal', build_temporal(package))),
        ("issued", extras.get('release_date', extras.get("Date Released", None))),
        ('accrualPeriodicity', extras.get('accrual_periodicity', None)),
        ('language', extras.get('language', None)),
        ("dataQuality", extras.get('data_quality', True)),
        ("landingPage", extras.get('homepage_url', package["url"])),
         ('rssFeed', extras.get('rss_feed', None)),
         ('systemOfRecords', extras.get('system_of_records', None)),
         ('systemOfRecordsNoneRelatedToThisDataset', extras.get('system_of_records_none_related_to_this_dataset', None)),
         ("distribution",
          #TODO distribution should hide any key/value pairs where value is "" or None (e.g. format)
         [
              OrderedDict([
                  ("accessURL", r["url"]),
                  ("format", r["format"]),
              ])
              for r in package["resources"]
         ])]

        theme = string.strip(extras.get('category', ""))
        if theme:
            retlist.append(
                ('theme', [string.strip(x) for x in string.split(theme, ',')])
            )

        references = string.strip(extras.get('related_documents', ""))
        if references:
            retlist.append(
                ('references', [string.strip(x) for x in string.split(references, ',')])
            )

    except KeyError as e:
        log.warn("Invalid field detected for package with id=[%s], title=['%s']: '%s'", package.get('id', None), package.get('title', None), e)
        return

    #TODO this is a lazy hack to make sure we don't have redundant fields when the free form key/value pairs are added
    extras_to_filter_out = ['publisher', 'contact_name','contact_email', 'unique_id', 'public_access_level',
                            'data_dictionary', 'bureau_code', 'program_code', 'access_level_comment', 'license_title',
                            'spatial', 'temporal', 'release_date', 'accrual_periodicity', 'language', 'granularity',
                            'data_quality', 'size', 'homepage_url', 'rss_feed', 'category', 'related_documents',
                            'system_of_records', 'system_of_records_none_related_to_this_dataset', 'tags',
                            'extrasRollup', 'format', 'accessURL']

    #Append any free extras (key/value pairs) that aren't part of common core but have been associated with the dataset
    #TODO really hackey, short on time, had to hardcode a lot of the names to remove. there's much better ways, maybe
    #generate a list of keys to ignore by calling a specific function to get the extras
    retlist_keys = [x for x,y in retlist]
    extras_keys = set(extras.keys()) - set(extras_to_filter_out)

    for key in extras_keys:
        convertedKey = underscore_to_camelcase(key)
        if convertedKey not in retlist_keys:
            retlist.append((convertedKey, extras[key]))

    # Remove entries where value is None, "", or empty list []
    striped_retlist = [(x, y) for x,y in retlist if y != None and y != "" and y != []]
    striped_retlist_keys = [x for x,y in striped_retlist]

    # If a required metadata field was removed, return empty string
    for required_field in ["title", "description", "keyword", "modified", "publisher", "contactPoint", "mbox", "identifier", "accessLevel"]:
        if required_field not in striped_retlist_keys:
            log.warn("Missing required field detected for package with id=[%s], title=['%s']: '%s'", package.get('id', None), package.get('title', None), required_field)
            return

    return OrderedDict(striped_retlist)

    
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

