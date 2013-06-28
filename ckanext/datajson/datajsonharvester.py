import urllib2

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_title_to_name

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
from ckanext.harvest.harvesters.base import HarvesterBase

import uuid

import logging
log = logging.getLogger(__name__)

class DataJsonHarvester(HarvesterBase):
    '''
    A Harvester for /data.json files.
    '''

    context = { "user": "harvest", "ignore_auth": True }

    def info(self):
        return {
            'name': 'datajson',
            'title': '/data.json',
            'description': 'Harvests remote /data.json files',
        }

    def validate_config(self, config):
        if not config:
            return config

        config_obj = json.loads(config)

        return config


    def gather_stage(self, harvest_job):
        log.debug('In datajson harvester gather_stage (%s)' % harvest_job.source.url)

        source = json.load(urllib2.urlopen(harvest_job.source.url))
        if len(source) == 0: return None

        # Loop through the packages we've already imported from this source
        # and go into their extra fields to get their source_datajson_identifier,
        # which corresponds to the /data.json 'identifier' field. Make a mapping.
        existing_datasets = { }
        for hobj in model.Session.query(HarvestObject).filter_by(source=harvest_job.source, current=True):
            try:
                pkg = get_action('package_show')(self.context, { "id": hobj.package_id })
            except:
                # reference is broken
                continue
            for extra in pkg["extras"]:
                if extra["key"] == "source_datajson_identifier":
                    existing_datasets[extra["value"]] = hobj.package_id
                    
        # If we've lost an association to the HarvestSource, scan all packages in the database.
        if True:
            for pkg in model.Session.query(Package):
                if pkg.extras.get("source_datajson_url") == harvest_job.source.url \
                    and pkg.extras.get("source_datajson_identifier"):
                        existing_datasets[pkg.extras["source_datajson_identifier"]] = pkg.id
                    
        # Create HarvestObjects for any records in the /data.json file.
            
        object_ids = []
        seen_datasets = set()
        
        for dataset in source:
            # Create a new HarvestObject for this identifier and save the
            # dataset metdata inside it for later.
            
            # Get the package_id of this resource if we've already imported
            # it into our system.
            
            pkg_id = existing_datasets.get(dataset["identifier"], uuid.uuid4().hex)
            seen_datasets.add(pkg_id)
            
            obj = HarvestObject(
                # reuse an existing package ID if we've already imported it
                guid=pkg_id,
                job=harvest_job,
                content=json.dumps(dataset))
            obj.save()
            object_ids.append(obj.id)
            
        # Remove packages no longer in the /data.json file.
        for id in existing_datasets.values():
            if id not in seen_datasets:
                print "deleting", id
                Session.query(Package).filter(Package.id == id)
            
        return object_ids

    def fetch_stage(self, harvest_object):
        # Nothing to do in this stage because we captured complete
        # dataset metadata from the first request to the /data.json file.
        return True

    def import_stage(self, harvest_object):
        log.debug('In datajson import_stage')

        dataset = json.loads(harvest_object.content)
        
        pkg = {
            "name": self.make_package_name(dataset["title"], harvest_object.guid),
            "title": dataset["title"],
            "extras": [{
                "key": "source_datajson_url",
                "value": harvest_object.source.url,
                },
                {
                "key": "source_datajson_identifier",
                "value": dataset["identifier"],
                }]
        }
    
        # Try to update an existing package with the ID set in harvest_object.guid.
        if model.Session.query(Package).filter_by(id=harvest_object.guid).first():
            # This ID is a package.
            print "updating", pkg["name"]
            pkg["id"] = harvest_object.guid
            pkg = get_action('package_update')(self.context, pkg)
        else:
            # It doesn't exist yet. Create a new one.
            print "creating", pkg["name"]
            pkg = get_action('package_create')(self.context, pkg)
            print "\t", pkg["id"], pkg["name"]
        
        # Flag the other objects linking to this package as not current anymore
        for ob in model.Session.query(HarvestObject).filter_by(package_id=pkg["id"]):
            ob.current = False
            ob.save()

        # Flag this as the current harvest object
        harvest_object.package_id = pkg['id']
        harvest_object.current = True
        harvest_object.save()

        return True

    def make_package_name(self, title, exclude_existing_package):
        '''
        Creates a URL friendly name from a title

        If the name already exists, it will add some random characters at the end
        '''

        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        pkg_obj = Session.query(Package).filter(Package.name == name).filter(Package.id != exclude_existing_package).first()
        if pkg_obj:
            return name + str(uuid.uuid4())[:5]
        else:
            return name
  
