try:
    from collections import OrderedDict  # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

import logging, string

log = logging.getLogger('datajson.builder')

# TODO this file is pretty sloppy, needs cleanup and redundancies removed

def make_datajson_catalog(datasets):
    catalog = OrderedDict([
        ('conformsTo', 'https://project-open-data.cio.gov/v1.1/schema'),  # requred
        ('describedBy', 'https://project-open-data.cio.gov/v1.1/schema/catalog.json'),  # optional
        ('@context', 'https://project-open-data.cio.gov/v1.1/schema/data.jsonld'),  # optional
        ('@type', 'dcat:Catalog'),  # optional
        ('dataset', datasets),  # required
    ])
    return catalog


def make_datajson_entry(package):
    # extras is a list of dicts [{},{}, {}]. For each dict, extract the key, value entries into a new dict
    extras = dict([(x['key'], x['value']) for x in package['extras']])

    retlist = []
    # if resource format is CSV then convert it to text/csv
    # Resource format has to be in 'csv' format for automatic datastore push.
    for r in package["resources"]:
        if r["format"].lower() == "csv":
            r["format"] = "text/csv"

    try:
        retlist = [
            ("@type", "dcat:Dataset"),  # optional
            ("title", package["title"]),  # required
            ("description", extras["notes"]),  # required
            ("keyword", [t["display_name"] for t in package["tags"]]),  # required
            # ("modified", package["metadata_modified"]), #required
            ("modified", extras.get("modified", package["metadata_modified"])),  # required
            # ("publisher", extras.get('publisher', package['author'])),  #required #json schema changed since 1.1
            ("publisher", get_publisher_tree(package, extras)),  # required
            # ('contactPoint', extras['contact_name']),  #required  #json schema changed since 1.1
            ('contactPoint', OrderedDict([
                ('@type', 'vcard:Contact'),  # optional
                ('fn', extras['contact_name']),  # required
                ('hasEmail', 'mailto:' + extras['contact_email']),  #required
            ])),  # required
            # ('mbox', extras['contact_email']),  #required     # deprecated since json schema 1.1
            ("identifier", extras['unique_id']),  # required
            ("isPartOf", extras.get('parent_dataset', None)),  #optional  since 1.1
            ("accessLevel", extras['public_access_level']),  #required
            # ("dataDictionary", extras.get('data_dictionary', extras.get("Data Dictionary"))),   #deprecated since 1.1
            # ("describedBy", extras.get('data_dictionary', extras.get("Data Dictionary"))),
            # ("bureauCode", extras.get("bureau_code", None)),
            # ("programCode", extras.get("program_code", None)),
            # ("accessLevelComment", extras.get("access_level_comment", None)),   #deprecated since 1.1
            ("rights", extras.get("access_level_comment", None)),  #renamed from accessLevelComment since 1.1
            #DWC: why is this here? should be under distribution          ("accessURL", get_primary_resource(package).get("url", None)),
            # ("webService", get_api_resource(package).get("endpoint", None)),  #deprecated since 1.1
            #DWC: why is this here? should be under distribution        ("format", get_primary_resource(package).get("format", None)),
            ("license", extras.get("License Agreement", package['license_title'])),
            ("spatial", extras.get('spatial', extras.get("Geographic Scope", None))),
            ("temporal", extras.get('temporal', build_temporal(package))),
            ("issued", extras.get('release_date', extras.get("Date Released", None))),
            ('accrualPeriodicity', extras.get('accrual_periodicity', None)),
            # ('language', extras.get('language', None)),
            ("dataQuality", extras.get('data_quality', None)),
            ("primaryITInvestmentUII", extras.get('primary_it_investment_uii', None)),
            # ("describedByType", extras.get('describedByType', None)),
            ("landingPage", extras.get('homepage_url', package["url"])),
            ('rssFeed', extras.get('rss_feed', None)),
            ('systemOfRecords', extras.get('system_of_records', None)),
            ('systemOfRecordsNoneRelatedToThisDataset',
             extras.get('system_of_records_none_related_to_this_dataset', None)),
            ("distribution",
             #TODO distribution should hide any key/value pairs where value is "" or None (e.g. format)
             [
                 OrderedDict([
                     ('@type', 'dcat:Distribution'),  #optional
                     # ("accessURL", r["url"]),  #required-if-applicable    #deprecated since 1.1
                     ("downloadURL", r["url"]),  #required-if-applicable  #renamed from `accessURL` since 1.1
                     # ("format", r["format"]),  #optional    #deprecated since 1.1
                     ("mediaType", r["formatReadable"]),  #optional    #renamed from `format` since 1.1
                     ("format", r["format"]),  #optional    #added since 1.1
                     ("title", r["name"]),  #optional    #added since 1.1
                     ("description", r["notes"]),  #optional    #added since 1.1
                     ("conformsTo", r["conformsTo"]),  #optional    #added since 1.1
                     ("describedBy", r["describedBy"]),  #optional    #added since 1.1
                     ("describedByType", r["describedByType"]),  #optional    #added since 1.1
                 ])
                 for r in package["resources"]
             ])]

        for pair in [
            ('program_code', 'programCode'),
            ('bureau_code', 'bureauCode'),
            ('category', 'theme'),
            ('related_documents', 'references'),
            ('language', 'language')
        ]:
            split_multiple_entries(retlist, extras, pair)

    except KeyError as e:
        log.warn("Invalid field detected for package with id=[%s], title=['%s']: '%s'", package.get('id', None),
                 package.get('title', None), e)
        return

    # TODO this is a lazy hack to make sure we don't have redundant fields when the free form key/value pairs are added
    extras_to_filter_out = ['publisher', 'contact_name', 'contact_email', 'unique_id', 'public_access_level',
                            'data_dictionary', 'bureau_code', 'program_code', 'access_level_comment', 'license_title',
                            'spatial', 'temporal', 'release_date', 'accrual_periodicity', 'language', 'granularity',
                            'data_quality', 'size', 'homepage_url', 'rss_feed', 'category', 'related_documents',
                            'system_of_records', 'system_of_records_none_related_to_this_dataset', 'tags',
                            'extrasRollup', 'format', 'accessURL', 'notes', 'publisher_1', 'publisher_2', 'publisher_3',
                            'publisher_4', 'publisher_5']

    # Append any free extras (key/value pairs) that aren't part of common core but have been associated with the dataset
    # TODO really hackey, short on time, had to hardcode a lot of the names to remove. there's much better ways, maybe
    # generate a list of keys to ignore by calling a specific function to get the extras
    retlist_keys = [x for x, y in retlist]
    extras_keys = set(extras.keys()) - set(extras_to_filter_out)

    for key in extras_keys:
        convertedKey = underscore_to_camelcase(key)
        if convertedKey not in retlist_keys:
            retlist.append((convertedKey, extras[key]))

    # Remove entries where value is None, "", or empty list []
    striped_retlist = [(x, y) for x, y in retlist if y != None and y != "" and y != []]
    striped_retlist_keys = [x for x, y in striped_retlist]

    # If a required metadata field was removed, return empty string
    for required_field in ["title", "description", "keyword", "modified", "publisher", "contactPoint",
                           "identifier", "accessLevel"]:
        if required_field not in striped_retlist_keys:
            log.warn("Missing required field detected for package with id=[%s], title=['%s']: '%s'",
                     package.get('id', None), package.get('title', None), required_field)
            return

    # When saved from UI DataQuality value is stored as "on" instead of True.
    # Check if value is "on" and replace it with True.
    striped_retlist_dict = OrderedDict(striped_retlist)
    if striped_retlist_dict.get('dataQuality') == "on" \
            or striped_retlist_dict.get('dataQuality') == "true" \
            or striped_retlist_dict.get('dataQuality') == "True":
        striped_retlist_dict['dataQuality'] = True
    elif striped_retlist_dict.get('dataQuality') == "false" \
            or striped_retlist_dict.get('dataQuality') == "False":
        striped_retlist_dict['dataQuality'] = False

    return striped_retlist_dict


def extra(package, key, default=None):
    # Retrieves the value of an extras field.
    for extra in package["extras"]:
        if extra["key"] == key:
            return extra["value"]
    return default


def get_publisher_tree(package, extras):
    # Sorry guys
    # TODO refactor that to recursion? any refactor would be nice though
    tree = [
        ('@type', 'org:Organization'),  # optional
        ('name', extras.get('publisher', package['author'])),  # required
    ]
    if 'publisher_1' in extras and extras['publisher_1']:
        publisher1 = [
            ('@type', 'org:Organization'),  # optional
            ('name', extras['publisher_1']),  # required
        ]
        if 'publisher_2' in extras and extras['publisher_2']:
            publisher2 = [
                ('@type', 'org:Organization'),  # optional
                ('name', extras['publisher_2']),  # required
            ]
            if 'publisher_3' in extras and extras['publisher_3']:
                publisher3 = [
                    ('@type', 'org:Organization'),  # optional
                    ('name', extras['publisher_3']),  # required
                ]
                if 'publisher_4' in extras and extras['publisher_4']:
                    publisher4 = [
                        ('@type', 'org:Organization'),  # optional
                        ('name', extras['publisher_4']),  # required
                    ]
                    if 'publisher_5' in extras and extras['publisher_5']:
                        publisher5 = [
                            ('@type', 'org:Organization'),  # optional
                            ('name', extras['publisher_5']),  # required
                        ]
                        publisher4 += [('subOrganizationOf', publisher5)]
                    publisher3 += [('subOrganizationOf', publisher4)]
                publisher2 += [('subOrganizationOf', publisher3)]
            publisher1 += [('subOrganizationOf', publisher2)]
        tree += [('subOrganizationOf', publisher1)]

    return OrderedDict(tree)


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
    if len(resources) == 0: return {}
    resources.sort(key=lambda r: acceptable_formats.index(r["format"].lower()))
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


def split_multiple_entries(retlist, extras, names):
    found_element = string.strip(extras.get(names[0], ""))
    if found_element:
        retlist.append(
            (names[1], [string.strip(x) for x in string.split(found_element, ',')])
        )
