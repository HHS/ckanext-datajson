from ckanext.datajson.datajson_ckan_28 import DatasetHarvesterBase

VALIDATION_SCHEMA = [('', 'Project Open Data (Federal)'),
                     ('non-federal', 'Project Open Data (Non-Federal)'), ]

__all__ = ["DatasetHarvesterBase"]
