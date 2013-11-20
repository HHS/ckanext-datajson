from ckanext.datajson.harvester_base import DatasetHarvesterBase

import urllib2, json, re, datetime

class CmsDataNavigatorHarvester(DatasetHarvesterBase):
    '''
    A Harvester for the CMS Data Navigator catalog.
    '''

    HARVESTER_VERSION = "0.9al" # increment to force an update even if nothing has changed

    def info(self):
        return {
            'name': 'cms-data-navigator',
            'title': 'CMS Data Navigator',
            'description': 'Harvests CMS Data Navigator-style catalogs.',
        }

    def load_remote_catalog(self, harvest_job):
        catalog = json.load(urllib2.urlopen(harvest_job.source.url))
        for item in catalog:
            item["identifier"] = item["ID"]
            item["title"] = item["Name"].strip()
        return catalog
        
    def set_dataset_info(self, package, dataset, dataset_defaults):
        extra(package, "Agency", "Department of Health & Human Services")
        package["author"] = "Centers for Medicare & Medicaid Services"
        extra(package, "author_id", "http://healthdata.gov/id/agency/cms")
        extra(package, "Bureau Code", "009:38")
        package["title"] = dataset["Name"].strip()
        package["notes"] = dataset.get("Description")
        
        package["url"] = dataset.get("Address")
        extra(package, "Date Released", parsedate(dataset["HealthData"].get("DateReleased")))
        extra(package, "Date Updated", parsedate(dataset["HealthData"].get("DateUpdated")))
        extra(package, "Agency Program URL", dataset["HealthData"].get("AgencyProgramURL"))
        extra(package, "Subject Area 1", "Medicare")
        extra(package, "Unit of Analysis", dataset["HealthData"].get("UnitOfAnalysis"))
        extra(package, "Data Dictionary", dataset["HealthData"].get("DataDictionaryURL"))
        extra(package, "Coverage Period", dataset["HealthData"].get("Coverage Period"))
        extra(package, "Collection Frequency", dataset["HealthData"].get("Collection Frequency"))
        extra(package, "Geographic Scope", dataset["HealthData"].get("GeographicScope"))
        extra(package, "Contact Name", dataset["HealthData"].get("GenericContactName", None) or dataset["HealthData"].get("ContactName")) # 'X or Y' syntax returns Y if X is either None or the empty string
        extra(package, "Contact Email", dataset["HealthData"].get("GenericContactEmail", None) or dataset["HealthData"].get("ContactEmail"))
        extra(package, "License Agreement", dataset["HealthData"].get("DataLicenseAgreementURL"))
        
        from ckan.lib.munge import munge_title_to_name
        package["tags"] = [ { "name": munge_title_to_name(t["Name"]) } for t in dataset.get("Keywords", [])]
        
        
def extra(package, key, value):
    if not value: return
    package.setdefault("extras", []).append({ "key": key, "value": value })
    
def parsedate(msdate):
    if not msdate: return None
    if msdate == "/Date(-62135575200000-0600)/": return None # is this "zero"?
    m = re.match(r"/Date\((\d+)([+\-]\d\d\d\d)\)\/", msdate)
    try:
        if not m: raise Exception("Invalid format.")
        isodate = datetime.datetime.fromtimestamp(long(m.group(1))/1000).isoformat().replace("T", " ")
    except e:
        print "Invalid date in CMS Data Navigator: %s (%s)" % (msdate, str(e))
        return None
    # We're ignoring the time zone offset because our HHS metadata format does not
    # support it, until we check on how Drupal indexing will handle it.
    return isodate
    
