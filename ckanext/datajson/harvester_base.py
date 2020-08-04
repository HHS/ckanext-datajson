from ckan import plugins as p

if p.toolkit.check_ckan_version(min_version='2.8.0'):
    from ckanext.datajson.datajson_ckan_28 import DatasetHarvesterBase
else:
    from ckanext.datajson.datajson_ckan_23 import DatasetHarvesterBase

VALIDATION_SCHEMA = [('', 'Project Open Data (Federal)'),
                     ('non-federal', 'Project Open Data (Non-Federal)'),]

__all__ = ["DatasetHarvesterBase"]
