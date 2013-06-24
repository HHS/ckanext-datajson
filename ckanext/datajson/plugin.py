import ckan.plugins as p

from ckan.lib.base import BaseController, render, config
from webhelpers.html import literal
from pylons import c, request, response
import collections, json, re

import ckan.model
from ckan.logic.action.get import current_package_list_with_resources

from build_datajson import make_datajson_entry
from build_datajsonld import dataset_to_jsonld

class DataJsonPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IRoutes, inherit=True)
    
    def before_map(self, m):
        return m
    
    def after_map(self, m):
        path = config.get("ckanext.datajson.path", "/data.json")
        ld_path = config.get("ckanext.datajsonld.path", re.sub(r"\.json$", ".jsonld", path))
        m.connect('datajson', path, controller='ckanext.datajson.plugin:DataJsonController', action='generate_json')
        m.connect('datajsonld', ld_path, controller='ckanext.datajson.plugin:DataJsonController', action='generate_jsonld')
        return m

class DataJsonController(BaseController):
    def generate_output(self, format):
        # set content type (charset required or pylons throws an error)
        response.content_type = 'application/json; charset=UTF-8'
        
        # allow caching of response (e.g. by Apache)
        del response.headers["Cache-Control"]
        del response.headers["Pragma"]
        
        # output
        data = make_json()
        
        if format == 'json-ld':
            # Convert this to JSON-LD.
            data = collections.OrderedDict([
                ("@context", collections.OrderedDict([
                    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
                    ("dcterms", "http://purl.org/dc/terms/"),
                    ("dcat", "http://www.w3.org/ns/dcat#"),
                    ("foaf", "http://xmlns.com/foaf/0.1/"),
                    ])
                ),
                ("@id", config.get("ckanext.datajsonld.id", config.get("ckan.site_url"))),
                ("@type", "dcat:Catalog"),
                ("dcterms:title", config.get("ckan.site_title", "Catalog")),
                ("rdfs:label", config.get("ckan.site_title", "Catalog")),
                ("foaf:homepage", config.get("ckan.site_url")),
                ("dcat:dataset", [dataset_to_jsonld(d) for d in data]),
            ])
            
        return literal(json.dumps(data))

    def generate_json(self):
        return self.generate_output('json')
        
    def generate_jsonld(self):
        return self.generate_output('json-ld')

def make_json():
    # Build the data.json file.
    packages = current_package_list_with_resources( { "model": ckan.model}, {})
    return [make_datajson_entry(p) for p in packages]
    

