from ckanext.datajson.harvester_base import DatasetHarvesterBase

import urllib2, json

class DataJsonHarvester(DatasetHarvesterBase):
    '''
    A Harvester for /data.json files.
    '''

    HARVESTER_VERSION = "0.9al" # increment to force an update even if nothing has changed

    def info(self):
        return {
            'name': 'datajson',
            'title': '/data.json',
            'description': 'Harvests remote /data.json files',
        }

    def load_remote_catalog(self, harvest_job):
        return json.load(urllib2.urlopen(harvest_job.source.url))
        
    def set_dataset_info(self, pkg, dataset, dataset_defaults):
        from parse_datajson import parse_datajson_entry
        parse_datajson_entry(dataset, pkg, dataset_defaults)
    

