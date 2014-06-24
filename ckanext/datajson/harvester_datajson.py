from ckanext.datajson.harvester_base import DatasetHarvesterBase
from ckanext.datajson.harvester_base import log

import urllib2, json

class DataJsonHarvester(DatasetHarvesterBase):
    '''
    A Harvester for /data.json files.
    '''

    HARVESTER_VERSION = "0.9ap" # increment to force an update even if nothing has changed

    def info(self):
        return {
            'name': 'datajson',
            'title': '/data.json',
            'description': 'Harvests remote /data.json files',
        }

    def load_remote_catalog(self, harvest_job):
	try:
	        datasets = json.load(urllib2.urlopen(harvest_job.source.url, None, 90))
	except urllib2.URLError as e:
		log.warn('Failed to fetch %s' % harvest_job.source.url)
		return []
	except ValueError as e:
		log.warn('Failed to parse %s' % harvest_job.source.url)
		return []


        # The first dataset should be for the data.json file itself. Check that
        # it is, and if so rewrite the dataset's title because Socrata exports
        # these items all with the same generic name that is confusing when
        # harvesting a bunch from different sources. It should have an accessURL
        # but Socrata fills the URL of these in under webService.
        if datasets != None and len(datasets) > 0 and (datasets[0].get("accessURL") == harvest_job.source.url
            or datasets[0].get("webService") == harvest_job.source.url) and \
            datasets[0].get("title") == "Project Open Data, /data.json file":
            datasets[0]["title"] = "%s Project Open Data data.json File" % harvest_job.source.title

        return datasets
        
    def set_dataset_info(self, pkg, dataset, harvester_config):
        from pod_to_package import parse_datajson_entry
        parse_datajson_entry(dataset, pkg, harvester_config)
    

