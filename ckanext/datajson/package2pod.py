try:
    from collections import OrderedDict  # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from logging import getLogger

log = getLogger(__name__)

from helpers import *
from datajsonvalidator import do_validation


class Package2Pod:
    def __init__(self):
        pass

    seen_identifiers = None

    @staticmethod
    def wrap_json_catalog(datasets, json_export_map):
        catalog_headers = [(x, y) for x, y in json_export_map.get('catalog_headers').iteritems()]
        catalog = OrderedDict(
            catalog_headers + [('dataset', datasets)]
        )
        return catalog

    @staticmethod
    def convert_package(package, json_export_map):
        dataset_dict = Package2Pod.export_map_fields(package, json_export_map)
        return Package2Pod.validate(package, dataset_dict)
        # return dataset_dict

    @staticmethod
    def export_map_fields(package, json_export_map):
        import string
        import sys, os

        try:
            dataset = json_export_map.get('dataset_headers')
            if not dataset:
                dataset = {}

            json_fields = json_export_map.get('dataset_fields_map')

            for key, field_map in json_fields.iteritems():
                # log.debug('%s => %s', key, field_map)

                field_type = field_map.get('type', 'direct')
                is_extra = field_map.get('extra')
                array_key = field_map.get('array_key')
                field = field_map.get('field')
                split = field_map.get('split')
                wrapper = field_map.get('wrapper')

                if 'direct' == field_type and field:
                    if is_extra:
                        dataset[key] = strip_if_string(get_extra(package, field))
                    else:
                        dataset[key] = strip_if_string(package.get(field))

                elif 'array' == field_type:
                    if is_extra:
                        if split:
                            found_element = strip_if_string(get_extra(package, field))
                            if found_element:
                                dataset[key] = [strip_if_string(x) for x in string.split(found_element, ',')]
                    else:
                        if array_key:
                            dataset[key] = [strip_if_string(t[array_key]) for t in package.get(field)]
                if wrapper:
                    method = getattr(Wrappers, wrapper)
                    if method:
                        Wrappers.pkg = package
                        Wrappers.field_map = field_map
                        dataset[key] = method(dataset.get(key))
            return dataset
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.error("%s : %s : %s", exc_type, filename, exc_tb.tb_lineno)
            raise e

    @staticmethod
    def validate(package, dataset_dict):
        import sys, os

        try:
            # CKAN doesn't like empty values on harvest, let's get rid of them
            # Remove entries where value is None, "", or empty list []
            dataset_dict = [(x, y) for x, y in dataset_dict.iteritems() if y is not None and y != "" and y != []]

            # When saved from UI DataQuality value is stored as "on" instead of True.
            # Check if value is "on" and replace it with True.
            dataset_dict = OrderedDict(dataset_dict)
            if dataset_dict.get('dataQuality') == "on" \
                    or dataset_dict.get('dataQuality') == "true" \
                    or dataset_dict.get('dataQuality') == "True":
                dataset_dict['dataQuality'] = True
            elif dataset_dict.get('dataQuality') == "false" \
                    or dataset_dict.get('dataQuality') == "False":
                dataset_dict['dataQuality'] = False

            errors = []
            try:
                do_validation([dict(dataset_dict)], errors, Package2Pod.seen_identifiers)
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

            return dataset_dict
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            # raise Exception("%s : %s : %s" % (exc_type, filename, exc_tb.tb_lineno))
            log.error("%s : %s : %s", exc_type, filename, exc_tb.tb_lineno)
            raise e


class Wrappers:
    def __init__(self):
        pass

    pkg = None
    field_map = None

    @staticmethod
    def spatial_publisher(value):
        return OrderedDict([
            ("@type", "org:Organization"),
            ("name", get_responsible_party(get_extra(Wrappers.pkg, Wrappers.field_map.get('field'))))
        ])

    @staticmethod
    def inventory_publisher(value):
        global currentPackageOrg

        extras = dict([(x['key'], x['value']) for x in Wrappers.pkg['extras']])

        publisher = strip_if_string(extras.get(Wrappers.field_map.get('field')))
        if publisher is None:
            return None

        currentPackageOrg = publisher

        organization_list = list()
        organization_list.append([
            ('@type', 'org:Organization'),  # optional
            ('name', publisher),  # required
        ])

        for i in range(1, 6):
            pub_key = 'publisher_' + str(i)
            if pub_key in extras and extras[pub_key] and strip_if_string(extras[pub_key]):
                organization_list.append([
                    ('@type', 'org:Organization'),  # optional
                    ('name', strip_if_string(extras[pub_key])),  # required
                ])
                currentPackageOrg = extras[pub_key]

        size = len(organization_list)

        tree = False
        for i in range(0, size):
            if tree:
                organization_list[i] += [('subOrganizationOf', OrderedDict(tree))]
            tree = organization_list[i]

        return OrderedDict(tree)

    @staticmethod
    def catalog_publisher(value):
        return OrderedDict([
            ("@type", "org:Organization"),
            ("name", 'StubOrg')
        ])

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
        'weekly': 'R/P1W',
        'continual': 'R/PT1S',
        'fortnightly': 'R/P0.5M',
        'annually': 'R/P1Y',
        'biannualy': 'R/P0.5Y',
        'asneeded': 'irregular',
        'irregular': 'irregular',
        'notplanned': 'irregular',
        'unknown': 'irregular',
        'not updated': 'irregular'
    }

    @staticmethod
    def fix_accrual_periodicity(frequency):
        return Wrappers.accrual_periodicity_dict.get(str(frequency).lower().strip(), frequency)

    @staticmethod
    def build_contact_point(someValue):
        extras = dict([(x['key'], x['value']) for x in Wrappers.pkg['extras']])

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
