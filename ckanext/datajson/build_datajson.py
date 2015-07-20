try:
    from collections import OrderedDict  # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

import json
from logging import getLogger

import os
import dateutil.parser \
    as parser
import ckan.model as model
from ckanext.harvest.model import HarvestObject

from helpers import get_responsible_party, get_reference_date

log = getLogger(__name__)

from datajsonvalidator import do_validation


def get_facet_fields():
    # Return fields that we'd like to add to default CKAN faceting. This really has
    # nothing to do with exporting data.json but it's probably a common consideration.
    facets = OrderedDict()

    # using "author" produces weird results because the Solr schema indexes it as "text" rather than "string"
    facets["Agency"] = "Publishers"
    # search facets remove spaces from field names
    facets["SubjectArea1"] = "Subjects"
    return facets


# def make_datajson_entry(package):
#     return OrderedDict([
#         ("title", package["title"]),
#         ("description", package["notes"]),
#         ("keyword", [t["display_name"] for t in package["tags"]]),
#         ("modified", extra(package, "Date Updated")),
#         ("publisher", package["author"]),
#         ("bureauCode", extra(package, "Bureau Code").split(" ") if extra(package, "Bureau Code") else None),
#         ("programCode", extra(package, "Program Code").split(" ") if extra(package, "Program Code") else None),
#         ("contactPoint", extra(package, "Contact Name")),
#         ("mbox", extra(package, "Contact Email")),
#         ("identifier", package["id"]),
#         ("accessLevel", extra(package, "Access Level", default="public")),
#         ("accessLevelComment", extra(package, "Access Level Comment")),
#         ("dataDictionary", extra(package, "Data Dictionary")),
#         ("accessURL", get_primary_resource(package).get("url", None)),
#         ("webService", get_api_resource(package).get("url", None)),
#         ("format", extension_to_mime_type(get_primary_resource(package).get("format", None))),
#         ("license", extra(package, "License Agreement")),
#         ("spatial", extra(package, "Geographic Scope")),
#         ("temporal", build_temporal(package)),
#         ("issued", extra(package, "Date Released")),
#         ("accrualPeriodicity", extra(package, "Publish Frequency")),
#         ("language", extra(package, "Language")),
#         ("PrimaryITInvestmentUII", extra(package, "PrimaryITInvestmentUII")),
#         ("granularity", "/".join(
#             x for x in [extra(package, "Unit of Analysis"), extra(package, "Geographic Granularity")] if
#             x is not None)),
#         ("dataQuality", extra(package, "Data Quality Met", default="true") == "true"),
#         ("theme", [s for s in (
#             extra(package, "Subject Area 1"), extra(package, "Subject Area 2"), extra(package, "Subject Area 3")
#         ) if s is not None]),
#
#         ("references", [s for s in [extra(package, "Technical Documentation")] if s is not None]),
#         ("landingPage", package["url"]),
#         ("systemOfRecords", extra(package, "System Of Records")),
#         ("distribution",
#          [
#              OrderedDict([
#                  ("identifier", r["id"]),  # NOT in POD standard, but useful for conversion to JSON-LD
#                  ("accessURL", r["url"]),
#                  ("format", r.get("mimetype", extension_to_mime_type(r["format"]))),
#              ])
#              for r in package["resources"]
#              if r["format"].lower() not in ("api", "query tool", "widget")
#              ]),
#     ])


# def extra(package, key, default=None):
#     # Retrieves the value of an extras field.
#     for xtra in package["extras"]:
#         if xtra["key"] == key:
#             return xtra["value"]
#     return default


def get_best_resource(package, acceptable_formats, unacceptable_formats=None):
    resources = list(r for r in package["resources"] if r["format"].lower() in acceptable_formats)
    if len(resources) == 0:
        if unacceptable_formats:
            # try at least any resource that's not unacceptable
            resources = list(r for r in package["resources"] if r["format"].lower() not in unacceptable_formats)
        if len(resources) == 0:
            # there is no acceptable resource to show
            return {}
    else:
        resources.sort(key=lambda r: acceptable_formats.index(r["format"].lower()))
    return resources[0]


def get_primary_resource(package):
    # Return info about a "primary" resource. Select a good one.
    return get_best_resource(package, ("csv", "xls", "xml", "text", "zip", "rdf"), ("api", "query tool", "widget"))


def get_api_resource(package):
    # Return info about an API resource.
    return get_best_resource(package, ("api", "query tool"))


def build_temporal(package):
    # Build one dataset entry of the data.json file.
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


currentPackageOrg = None


class JsonExportBuilder:
    def __init__(self):
        global currentPackageOrg
        currentPackageOrg = None

    @staticmethod
    def make_datajson_export_catalog(datasets):
        catalog = OrderedDict([
            ('conformsTo', 'https://project-open-data.cio.gov/v1.1/schema'),  # requred
            ('describedBy', 'https://project-open-data.cio.gov/v1.1/schema/catalog.json'),  # optional
            ('@context', 'https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld'),  # optional
            ('@type', 'dcat:Catalog'),  # optional
            ('dataset', datasets),  # required
        ])
        return catalog

    @staticmethod
    def export_map_fields(package, json_export_map):
        import sys, os, string

        try:
            dataset = {}
            for key, field_map in json_export_map.iteritems():
                # log.debug('%s => %s', key, field_map)

                field_type = field_map.get('type', 'direct')
                is_extra = field_map.get('extra')
                array_key = field_map.get('array_key')
                field = field_map['field']
                split = field_map.get('split')

                if 'direct' == field_type:
                    if is_extra:
                        dataset[key] = strip_if_string(extra(package, field))
                    else:
                        dataset[key] = strip_if_string(package.get(field))
                elif 'array' == field_type:
                    if is_extra:
                        if split:
                            found_element = strip_if_string(extra(package, field))
                            if found_element:
                                # if key in ['bureauCode', 'programCode']:
                                #     log.debug("found %s in %s : %s", field, package.get('id'), found_element)
                                dataset[key] = [strip_if_string(x) for x in string.split(found_element, ',')]
                            # elif key in ['bureauCode', 'programCode']:
                            #     log.debug("not found %s in %s", field, package.get('id'))
                            #     log.debug("%s", package.get('extras'))
                    else:
                        if array_key:
                            dataset[key] = [strip_if_string(t[array_key]) for t in package.get(field)]
                            # else:
                            #     dataset[key] = package[field]
            return dataset
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.debug("%s : %s : %s", exc_type, fname, exc_tb.tb_lineno)
            raise e

    @staticmethod
    def make_datajson_export_entry(package, json_export_map, seen_identifiers):
        import sys, os

        try:

            global currentPackageOrg
            currentPackageOrg = None

            # Map data using json export map
            dataset = JsonExportBuilder.export_map_fields(package, json_export_map)

            # extras is a list of dicts [{},{}, {}]. For each dict, extract the key, value entries into a new dict
            extras = dict([(x['key'], x['value']) for x in package['extras']])

            parent_dataset_id = extras.get('parent_dataset')
            if parent_dataset_id:
                parent = model.Package.get(parent_dataset_id)
                parent_uid = parent.extras.col.target['unique_id'].value
                if parent_uid:
                    parent_dataset_id = parent_uid

            # if resource format is CSV then convert it to text/csv
            for r in package.get('resources', []):
                if r["format"].lower() == "csv":
                    r["format"] = "text/csv"
                if r["format"].lower() == "json":
                    r["format"] = "application/json"
                if r["format"].lower() == "pdf":
                    r["format"] = "application/pdf"

            #
            # check to see if this is a spatial record
            # if so, we want to do a crosswalk out of the metadata extras
            #
            log.warn("determine spatial or not [%s]", strip_if_string(package["title"]))
            date = extra(package, "Metadata Date")
            log.warn("date %s", date)
            if date:
                log.warn("treating this as spatial data")
                harvest_object = model.Session.query(HarvestObject) \
                    .filter(HarvestObject.package_id == package['id']) \
                    .filter(HarvestObject.current is True) \
                    .first()
                accessLevel = extra(package, "Access Level", "public")
                accrualPeriodicity = JsonExportBuilder.get_accrual_periodicity_spatial(
                    extra(package, "Frequency Of Update"))
                dataQuality = extra(package, 'Data Quality')
                conformsTo = strip_if_string(extra(package, 'Data Standard'))
                describedBy = strip_if_string(extra(package, 'Data Dictionary'))
                describedByType = strip_if_string(extra(package, 'Data Dictionary Type'))
                description = strip_if_string(extra(package, 'Description'))
                if not description:
                    description = strip_if_string(extra(package, 'Abstract'))
                if not description:
                    description = strip_if_string(package["notes"])
                identifier = strip_if_string(extra(package, 'Guid'))
                if not identifier:
                    identifier = strip_if_string(package["title"])
                if not identifier:
                    identifier = package["id"]
                issued = get_reference_date(extra(package, "Release Date"))
                keyword = tags(package)
                landingPage = strip_if_string(extra(package, "Homepage URL"))
                license = strip_if_string(extra(package, "License"))
                modified = None
                referencedate = json.loads(extra(package, "Dataset Reference Date"))
                if referencedate and isinstance(referencedate, list):
                    for date_type in ["revision", "publication"]:
                        for ref in referencedate:
                            modified = ref['value'] if ref['type'] == date_type else None
                    if not modified:
                        modified = referencedate[0]['value']
                if not modified:
                    modified = clean_date(extra(package, "Last Update"))
                if not modified:
                    modified = clean_date(extra(package, "Metadata Date"))
                # log.warn("modified: %s",modified)
                primaryITInvestmentUII = strip_if_string(extra(package, 'Primary_IT_Investment_UII'))
                #
                # this doesn't match crosswalk -- look at it
                #
                publisher = OrderedDict([
                    ("@type", "org:Organization"),
                    ("name", get_responsible_party(extra(package, "Responsible Party")))
                ])  # required
                rights = strip_if_string(extra(package, 'Rights'))
                spatial = strip_if_string(extra(package, 'Spatial'))
                systemOfRecords = strip_if_string(extra(package, 'System of Records'))
                # how do we represent from a time, to present?
                temporalbegin = extra(package, 'Temporal Extent Begin')
                temporalend = extra(package, 'Temporal Extent End')
                if temporalbegin and temporalend:
                    tb = clean_date(temporalbegin)
                    te = clean_date(temporalend)
                    if tb and te:
                        temporal = tb + '/' + te
                    else:
                        temporal = ""
                else:
                    temporal = ""
                bureauCode = [bureau_code(package)]
                programCodePart = extra(package, 'Program Code')
                if not programCodePart:
                    programCodePart = program_code(harvest_object)
                if not programCodePart:
                    programCodePart = "000:000"
                programCode = [programCodePart]
                #
                # are these arrays in the ISO metadata? Should we be pulling them apart somehow?
                language = [convert_language(strip_if_string(extra(package, 'Metadata Language', "")))]
                log.warn("language = %s %s", language,
                         strip_if_string(extra(package, 'Metadata Language', "")))
                if extra(package, 'Related Documents'):
                    references = [extra(package, 'Related Documents', "")]
                else:
                    references = None
                if extra(package, 'Category'):
                    theme = [extra(package, 'Category', "")]
                else:
                    theme = None
            else:
                log.warn("treating this as non-spatial harvested data")
                # date = extra(package, "Date Updated")
                # accessLevel = strip_if_string(extras.get('public_access_level'))
                # dataQuality = strip_if_string(extras.get('data_quality'))
                # conformsTo = strip_if_string(extras.get('conforms_to'))
                # describedBy = strip_if_string(extras.get('data_dictionary'))
                # describedByType = strip_if_string(extras.get('data_dictionary_type'))
                # description = strip_if_string(package["notes"])
                # identifier = strip_if_string(extras.get('unique_id'))
                # keyword = [t["display_name"] for t in package["tags"]]
                # issued = strip_if_string(extras.get('release_date'))
                # landingPage = strip_if_string(extras.get('homepage_url'))
                # license = strip_if_string(extras.get("license_new"))
                # primaryITInvestmentUII = strip_if_string(extras.get('primary_it_investment_uii'))
                # rights = strip_if_string(extras.get('access_level_comment'))
                # spatial = strip_if_string(package.get("spatial"))
                # temporal = strip_if_string(extras.get('temporal'))
                # modified = strip_if_string(extras.get("modified"))
                # bureauCode = [string.strip(x) for x in string.split(extras.get('bureau_code', ""), ',')]
                # language = [string.strip(x) for x in string.split(extras.get('language', ""), ',')]
                # programCode = [string.strip(x) for x in string.split(extras.get('program_code', ""), ',')]
                # references = [string.strip(x) for x in string.split(extras.get('related_documents', ""), ',')]
                # theme = [string.strip(x) for x in string.split(extras.get('category', ""), ',')]
                accrualPeriodicity = JsonExportBuilder.get_accrual_periodicity(extras.get('accrual_periodicity'))
                publisher = JsonExportBuilder.get_publisher_tree_wrong_order(extras)
                # systemOfRecords = strip_if_string(extras.get('system_of_records'))

            try:
                retlist = [
                    ("@type", "dcat:Dataset"),  # optional
                    ("title", dataset.get('title')),  # required
                    ("accessLevel", dataset.get('accessLevel')),  # required
                    ('accrualPeriodicity', accrualPeriodicity),  # optional
                    ("conformsTo", dataset.get('conformsTo')),  # optional
                    ('contactPoint', JsonExportBuilder.get_contact_point(extras)),  # required
                    ("dataQuality", dataset.get('dataQuality')),  # required-if-applicable
                    ("describedBy", dataset.get('describedBy')),  # optional
                    ("describedByType", dataset.get('describedByType')),  # optional
                    ("description", dataset.get('description')),  # required
                    ("identifier", dataset.get('identifier')),  # required
                    ("isPartOf", parent_dataset_id),  # optional
                    ("issued", dataset.get('issued')),  # optional
                    ("keyword", dataset.get('keyword')),  # required
                    ("landingPage", dataset.get('landingPage')),  # optional
                    ("license", dataset.get('license')),  # required-if-applicable
                    ("modified", dataset.get('modified')),  # required
                    ("primaryITInvestmentUII", dataset.get('primaryITInvestmentUII')),  # optional
                    ("publisher", publisher),  # required
                    ("rights", dataset.get('rights')),  # required
                    ("spatial", dataset.get('spatial')),  # required-if-applicable
                    ('systemOfRecords', dataset.get('systemOfRecords')),  # optional
                    ("temporal", dataset.get('temporal')),  # required-if-applicable
                    ("distribution", JsonExportBuilder.generate_distribution(package)),  # required-if-applicable
                    ("bureauCode", dataset.get('bureauCode')),  # required
                    ("language", dataset.get('language')),  # optional
                    ("programCode", dataset.get('programCode')),  # required
                    ("references", dataset.get('references')),  # optional
                    ("theme", dataset.get('theme'))  # optional
                ]

            except KeyError as e:
                log.warn("Missing Required Field for package with id=[%s], title=['%s'], organization=['%s']: '%s'" % (
                    package.get('id'), package.get('title'), currentPackageOrg, e))

                errors = ['Missing Required Field', ["%s" % e]]
                errors_dict = OrderedDict([
                    ('id', package.get('id')),
                    ('name', package.get('name')),
                    ('title', package.get('title')),
                    ('organization', currentPackageOrg),
                    ('errors', errors),
                ])

                return errors_dict

            # Remove entries where value is None, "", or empty list []
            striped_retlist = [(x, y) for x, y in retlist if y is not None and y != "" and y != []]

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

            errors = []
            try:
                do_validation([dict(striped_retlist_dict)], errors, seen_identifiers)
            except Exception as e:
                errors.append(("Internal Error", ["Something bad happened: " + unicode(e)]))
            if len(errors) > 0:
                for error in errors:
                    log.warn(error)

                errors_dict = OrderedDict([
                    ('id', package.get('id')),
                    ('name', package.get('name')),
                    ('title', package.get('title')),
                    ('organization', currentPackageOrg),
                    ('errors', errors),
                ])

                return errors_dict

            return striped_retlist_dict
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.debug("%s : %s : %s", exc_type, fname, exc_tb.tb_lineno)
            raise e

    # used by get_accrual_periodicity
    accrual_periodicity_dict = {
        'completely irregular': 'irregular',
        'decennial': 'R/P10Y',
        'quadrennial': 'R/P4Y',
        'annual': 'R/P1Y',
        'bimonthly': 'R/P2M',  # or R/P0.5M
        'semiweekly': 'R/P3.5D',
        'daily': 'R/P1D',
        'biweekly': 'R/P2W',  # or R/P0.5W
        'semiannual': 'R/P6M',
        'biennial': 'R/P2Y',
        'triennial': 'R/P3Y',
        'three times a week': 'R/P0.33W',
        'three times a month': 'R/P0.33M',
        'continuously updated': 'R/PT1S',
        'monthly': 'R/P1M',
        'quarterly': 'R/P3M',
        'semimonthly': 'R/P0.5M',
        'three times a year': 'R/P4M',
        'weekly': 'R/P1W'
    }

    # used by get_accrual_periodicity_spatial
    accrual_periodicity_spatial_dict = {
        'continual': 'R/PT1S',
        'daily': 'R/P1D',
        'weekly': 'R/P1W',
        'fortnightly': 'R/P0.5M',
        'annually': 'R/P1Y',
        'monthly': 'R/P1M',
        'quarterly': 'R/P3M',
        'biannualy': 'R/P0.5Y',
        'asneeded': 'irregular',
        'irregular': 'irregular',
        'notplanned': 'irregular',
        'unknown': 'irregular',
        'not updated': 'irregular'
    }

    @staticmethod
    def get_accrual_periodicity(frequency):
        return JsonExportBuilder.accrual_periodicity_dict.get(str(frequency).lower().strip(), frequency)

    @staticmethod
    def get_accrual_periodicity_spatial(frequency):
        return JsonExportBuilder.accrual_periodicity_spatial_dict.get(str(frequency).lower().strip(), frequency)

    @staticmethod
    def generate_distribution(package):
        arr = []
        for r in package["resources"]:
            resource = [("@type", "dcat:Distribution")]
            rkeys = r.keys()
            if 'url' in rkeys:
                res_url = strip_if_string(r.get('url'))
                if res_url:
                    res_url = res_url.replace('http://[[REDACTED', '[[REDACTED')
                    res_url = res_url.replace('http://http', 'http')
                    if 'api' == r.get('resource_type') or 'accessurl' == r.get('resource_type'):
                        resource += [("accessURL", res_url)]
                    else:
                        resource += [("downloadURL", res_url)]
                        if 'format' in rkeys:
                            res_format = strip_if_string(r.get('format'))
                            if res_format:
                                resource += [("mediaType", res_format)]
                        else:
                            log.warn("Missing mediaType for resource in package ['%s']", package.get('id'))
            else:
                log.warn("Missing downloadURL for resource in package ['%s']", package.get('id'))

            # if 'accessURL_new' in rkeys:
            # res_access_url = strip_if_string(r.get('accessURL_new'))
            # if res_access_url:
            # resource += [("accessURL", res_access_url)]

            if 'formatReadable' in rkeys:
                res_attr = strip_if_string(r.get('formatReadable'))
                if res_attr:
                    resource += [("format", res_attr)]

            if 'name' in rkeys:
                res_attr = strip_if_string(r.get('name'))
                if res_attr:
                    resource += [("title", res_attr)]

            if 'notes' in rkeys:
                res_attr = strip_if_string(r.get('notes'))
                if res_attr:
                    resource += [("description", res_attr)]

            if 'conformsTo' in rkeys:
                res_attr = strip_if_string(r.get('conformsTo'))
                if res_attr:
                    resource += [("conformsTo", res_attr)]

            if 'describedBy' in rkeys:
                res_attr = strip_if_string(r.get('describedBy'))
                if res_attr:
                    resource += [("describedBy", res_attr)]

            if 'describedByType' in rkeys:
                res_attr = strip_if_string(r.get('describedByType'))
                if res_attr:
                    resource += [("describedByType", res_attr)]

            striped_resource = [(x, y) for x, y in resource if y is not None and y != "" and y != []]

            arr += [OrderedDict(striped_resource)]

        return arr

    @staticmethod
    def get_contact_point(extras):
        for required_field in ["contact_name", "contact_email"]:
            if required_field not in extras.keys():
                raise KeyError(required_field)

        fn = strip_if_string(extras['contact_name'])
        if fn is None:
            raise KeyError('contact_name')

        email = strip_if_string(extras['contact_email'])
        if email is None:
            raise KeyError('contact_email')

        if '[[REDACTED' not in email:
            if '@' not in email:
                raise KeyError('contact_email')
            else:
                email = 'mailto:' + email

        contact_point = OrderedDict([
            ('@type', 'vcard:Contact'),  # optional
            ('fn', fn),  # required
            ('hasEmail', email),  # required
        ])
        return contact_point

    # @staticmethod
    # def extra(package, key, default=None):
    #     # Retrieves the value of an extras field.
    #     for xtra in package["extras"]:
    #         if xtra["key"] == key:
    #             return xtra["value"]
    #     return default

    @staticmethod
    def get_publisher_tree_wrong_order(extras):
        global currentPackageOrg
        publisher = strip_if_string(extras.get('publisher'))
        if publisher is None:
            return None
            # raise KeyError('publisher')

        currentPackageOrg = publisher

        organization_list = list()
        organization_list.append([
            ('@type', 'org:Organization'),  # optional
            ('name', publisher),  # required
        ])

        for i in range(1, 6):
            key = 'publisher_' + str(i)
            if key in extras and extras[key] and strip_if_string(extras[key]):
                organization_list.append([
                    ('@type', 'org:Organization'),  # optional
                    ('name', strip_if_string(extras[key])),  # required
                ])
                currentPackageOrg = extras[key]

        size = len(organization_list)

        # [OSCIT, GSA]
        # organization_list.reverse()
        # [GSA, OSCIT]

        tree = False
        for i in range(0, size):
            if tree:
                organization_list[i] += [('subOrganizationOf', OrderedDict(tree))]
            tree = organization_list[i]

        return OrderedDict(tree)

    @staticmethod
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

    @staticmethod
    def get_best_resource(package, acceptable_formats):
        resources = list(r for r in package["resources"] if r["format"].lower() in acceptable_formats)
        if len(resources) == 0: return {}
        resources.sort(key=lambda r: acceptable_formats.index(r["format"].lower()))
        return resources[0]

    @staticmethod
    def get_primary_resource(package):
        # Return info about a "primary" resource. Select a good one.
        return JsonExportBuilder.get_best_resource(package, ("csv", "xls", "xml", "text", "zip", "rdf"))

    @staticmethod
    def get_api_resource(package):
        # Return info about an API resource.
        return JsonExportBuilder.get_best_resource(package, ("api", "query tool"))

        # @staticmethod
        # def split_multiple_entries(retlist, extras, names):
        #     found_element = string.strip(extras.get(names[1], ""))
        #     if found_element:
        #         retlist.append(
        #             (names[0], [string.strip(x) for x in string.split(found_element, ',')])
        #         )

    @staticmethod
    def strip_if_string(val):
        if isinstance(val, (str, unicode)):
            val = val.strip()
            if '' == val:
                val = None
        return val


def clean_date(val):
    try:
        if isinstance(val, (str, unicode)):
            log.debug("clean_date: val %s ", val)
            # 2014-03-18-06:00 needs to become "2014-03-18T06:00"
            date = (parser.parse(val))
            val = (date.isoformat())
    except Exception as e:
        log.debug("clean_date: exception %s val  %s ", e, val)
        val = ""
    return val


def extra(package, key, default=None):
    # Retrieves the value of an extras field.
    '''
    for extra in package["extras"]:
        if extra["key"] == "extras_rollup":
            extras_rollup_dict = extra["value"]
            #return(extras_rollup_dict) #returns full json-formatted 'value' field of extras_rollup
            extras_rollup_dict = json.loads(extra["value"])
            for rollup_key in extras_rollup_dict.keys():
                if rollup_key == key: return extras_rollup_dict.get(rollup_key)

    return default
    '''

    current_extras = package["extras"]
    # new_extras =[]
    new_extras = {}
    for extra in current_extras:
        if extra['key'] == 'extras_rollup':
            rolledup_extras = json.loads(extra['value'])
            for k, value in rolledup_extras.iteritems():
                # log.info("rolledup_extras key: %s, value: %s", k, value)
                # new_extras.append({"key": k, "value": value})
                new_extras[k] = value
        else:
            #    new_extras.append(extra)
            new_extras[extra['key']] = extra['value']

    # decode keys:
    for k, v in new_extras.iteritems():
        k = k.replace('_', ' ').replace('-', ' ').title()
        if isinstance(v, (list, tuple)):
            v = ", ".join(map(unicode, v))
        # log.info("decoded values key: %s, value: %s", k, v)
        if k == key:
            return v
    return default


def program_code(harvest_object, default=None):
    harvest_name = harvest_object.source.title if harvest_object else None
    # log.debug("harvest name: %s",harvest_name)
    file = open(os.path.join(os.path.dirname(__file__), "resources") + "/harvest-to-program-codes.json", 'r');
    codelist = json.load(file)
    for harvest_source in codelist:
        if harvest_source['Harvest Source Name'] == harvest_name:
            # log.debug("found match: %s", harvest_source["Program Code"])
            result = harvest_source["Program Code"];
            # log.debug("found program code match: '%s'", result)
            return result
    return default


def bureau_code(package, default=None):
    log.debug("org title: %s", package["organization"]["title"])
    file = open(os.path.join(os.path.dirname(__file__), "resources") + "/omb-agency-bureau-treasury-codes.json", 'r');
    codelist = json.load(file)
    for bureau in codelist:
        if bureau['Agency'] == package["organization"]["title"]:
            log.debug("found match: %s", "[{0}:{1}]".format(bureau["OMB Agency Code"], bureau["OMB Bureau Code"]))
            result = "{0}:{1}".format(bureau["OMB Agency Code"], bureau["OMB Bureau Code"])
            log.debug("found match: '%s'", result)
            return result
    return default


def tags(package, default=None):
    # Retrieves the value of an extras field.
    for extra in package["extras"]:
        if extra["key"] == "tags":
            keywords = extra["value"].split(",")
            keywords = map(unicode.strip, keywords)
            return keywords


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
        "arcgis_rest": "text/html",
        "wms": "text/html",
        "html": "text/html",
        "application/pdf": "application/pdf",
    }
    return ext.get(file_ext.lower(), "application/unknown")


def convert_language(isocode, default="en-US"):
    langcode = {
        "eng": "en-US",
        "spa": "es-US",
        "fre": "fr-CA",
    }
    return langcode.get(isocode, default)


def strip_if_string(val):
    if isinstance(val, (str, unicode)):
        val = val.strip()
        if '' == val:
            val = None
    return val

# "Convert the value of this field based on the following mapping //gmd:identificationInfo/gmd:MD_DataIdentification/gmd:language
# eng; USA - en-US
# spa; USA - es-US
# eng; CAN - en-CA
# fre; CAN - fr-CA
# spa; MEX - es-MX"
