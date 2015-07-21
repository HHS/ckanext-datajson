import logging

from pylons import config
from ckan import plugins as p
from ckan.lib import helpers as h
import re

REDACTED_REGEX = re.compile(
    r'^(\[\[REDACTED).*?(\]\])$'
)

log = logging.getLogger(__name__)


def get_reference_date(date_str):
    """
        Gets a reference date extra created by the harvesters and formats it
        nicely for the UI.

        Examples:
            [{"type": "creation", "value": "1977"}, {"type": "revision", "value": "1981-05-15"}]
            [{"type": "publication", "value": "1977"}]
            [{"type": "publication", "value": "NaN-NaN-NaN"}]

        Results
            1977 (creation), May 15, 1981 (revision)
            1977 (publication)
            NaN-NaN-NaN (publication)
    """
    try:
        out = []
        for date in h.json.loads(date_str):
            value = h.render_datetime(date['value']) or date['value']
            out.append('{0} ({1})'.format(value, date['type']))
        return ', '.join(out)
    except (ValueError, TypeError):
        return date_str


def get_responsible_party(value):
    """
        Gets a responsible party extra created by the harvesters and formats it
        nicely for the UI.

        Examples:
            [{"name": "Complex Systems Research Center", "roles": ["pointOfContact"]}]
            [{"name": "British Geological Survey", "roles": ["custodian", "pointOfContact"]},
             {"name": "Natural England", "roles": ["publisher"]}]

        Results
            Complex Systems Research Center (pointOfContact)
            British Geological Survey (custodian, pointOfContact); Natural England (publisher)
    """
    formatted = {
        'resourceProvider': p.toolkit._('Resource Provider'),
        'pointOfContact': p.toolkit._('Point of Contact'),
        'principalInvestigator': p.toolkit._('Principal Investigator'),
    }

    try:
        out = []
        parties = h.json.loads(value)
        for party in parties:
            roles = [formatted[role] if role in formatted.keys() else p.toolkit._(role.capitalize()) for role in
                     party['roles']]
            out.append('{0} ({1})'.format(party['name'], ', '.join(roles)))
        return '; '.join(out)
    except (ValueError, TypeError):
        return value


def get_common_map_config():
    """
        Returns a dict with all configuration options related to the common
        base map (ie those starting with 'ckanext.spatial.common_map.')
    """
    namespace = 'ckanext.spatial.common_map.'
    return dict([(k.replace(namespace, ''), v) for k, v in config.iteritems() if k.startswith(namespace)])


def strip_if_string(val):
    """
    :param val: any
    :return: str|None
    """
    if isinstance(val, (str, unicode)):
        val = val.strip()
        if '' == val:
            val = None
    return val


def get_extra(package, key, default=None):
    """
    Retrieves the value of an extras field.

    :param package: dict
    :param key: str
    :param default: Any
    :return: Any
    """

    import json

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


def get_export_map_json():
    """
    Reading json export map from file
    :return: obj
    """
    import os
    import json
    map_path = os.path.join(os.path.dirname(__file__), 'export_map', 'export.map.json')

    with open(map_path, 'r') as export_map_json:
        json_export_map = json.load(export_map_json)

    return json_export_map


def detect_publisher(extras):
    """
    Detect publisher by package extras
    :param extras: dict
    :return: str
    """
    publisher = None

    if 'publisher' in extras and extras['publisher']:
        publisher = strip_if_string(extras['publisher'])

    for i in range(1, 6):
        key = 'publisher_' + str(i)
        if key in extras and extras[key] and strip_if_string(extras[key]):
            publisher = strip_if_string(extras[key])
    return publisher


def is_redacted(value):
    return REDACTED_REGEX.match(value)
