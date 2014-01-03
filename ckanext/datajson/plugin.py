import ckan.plugins as p

from ckan.lib.base import BaseController, render, c
import ckan.model as model
from pylons import request, response
import ckan.lib.dictization.model_dictize as model_dictize
import json, re
import logging
from jsonschema.exceptions import best_match
import StringIO

logger = logging.getLogger('datajson')

def get_validator():
    import os
    from jsonschema import Draft4Validator, FormatChecker

    schema_path = os.path.join(os.path.dirname(__file__), 'schema', '1_0_final', 'single_entry.json')
    with open(schema_path, 'r') as file:
        schema = json.loads(file.read())
        return Draft4Validator(schema, format_checker=FormatChecker())

    logger.warn('Unable to create validator')
    return None

validator = get_validator()


try:
    from collections import OrderedDict # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from build_datajson import make_datajson_entry
# from build_enterprisedatajson import make_enterprisedatajson_entry
from build_datajsonld import dataset_to_jsonld

class DataJsonPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IConfigurer)
    p.implements(p.interfaces.IRoutes, inherit=True)
    
    def update_config(self, config):
    	# Must use IConfigurer rather than IConfigurable because only IConfigurer
    	# is called before after_map, in which we need the configuration directives
    	# to know how to set the paths.

        # TODO commenting out enterprise data inventory for right now
        # DataJsonPlugin.route_edata_path = config.get("ckanext.enterprisedatajson.path", "/enterprisedata.json")
        DataJsonPlugin.route_enabled = config.get("ckanext.datajson.url_enabled", "True")=='True'
        DataJsonPlugin.route_path = config.get("ckanext.datajson.path", "/data.json")
        DataJsonPlugin.route_ld_path = config.get("ckanext.datajsonld.path", re.sub(r"\.json$", ".jsonld", DataJsonPlugin.route_path))
        DataJsonPlugin.ld_id = config.get("ckanext.datajsonld.id", config.get("ckan.site_url"))
        DataJsonPlugin.ld_title = config.get("ckan.site_title", "Catalog")
        DataJsonPlugin.site_url = config.get("ckan.site_url")

        # Adds our local templates directory. It's smart. It knows it's
        # relative to the path of *this* file. Wow.
        p.toolkit.add_template_directory(config, "templates")

    def before_map(self, m):
        return m
    
    def after_map(self, m):

        if DataJsonPlugin.route_enabled:
            # /data.json and /data.jsonld (or other path as configured by user)
            m.connect('datajson', DataJsonPlugin.route_path, controller='ckanext.datajson.plugin:DataJsonController', action='generate_json')
            # TODO commenting out enterprise data inventory for right now
            #m.connect('enterprisedatajson', DataJsonPlugin.route_edata_path, controller='ckanext.datajson.plugin:DataJsonController', action='generate_enterprise')
            #m.connect('datajsonld', DataJsonPlugin.route_ld_path, controller='ckanext.datajson.plugin:DataJsonController', action='generate_jsonld')

        # TODO DWC update action
        # /data/{org}/data.json
        m.connect('public_data_listing', '/organization/{org}/data.json', controller='ckanext.datajson.plugin:DataJsonController', action='generate_pdl')

        # TODO DWC update action
        # /data/{org}/edi.json
        m.connect('enterprise_data_inventory', '/organization/{org}/edi.json', controller='ckanext.datajson.plugin:DataJsonController', action='generate_edi')

        # /pod/validate
        #m.connect('datajsonvalidator', "/pod/validate", controller='ckanext.datajson.plugin:DataJsonController', action='validator')
        
        return m

class DataJsonController(BaseController):
    def generate_output(self, format):
        # set content type (charset required or pylons throws an error)
        response.content_type = 'application/json; charset=UTF-8'
        
        # allow caching of response (e.g. by Apache)
        del response.headers["Cache-Control"]
        del response.headers["Pragma"]

        #TODO special processing for enterprise
        # output
        data = make_json()
        
        if format == 'json-ld':
            # Convert this to JSON-LD.
            data = OrderedDict([
                ("@context", OrderedDict([
                    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
                    ("dcterms", "http://purl.org/dc/terms/"),
                    ("dcat", "http://www.w3.org/ns/dcat#"),
                    ("foaf", "http://xmlns.com/foaf/0.1/"),
                    ])
                ),
                ("@id", DataJsonPlugin.ld_id),
                ("@type", "dcat:Catalog"),
                ("dcterms:title", DataJsonPlugin.ld_title),
                ("rdfs:label", DataJsonPlugin.ld_title),
                ("foaf:homepage", DataJsonPlugin.site_url),
                ("dcat:dataset", [dataset_to_jsonld(d) for d in data]),
            ])
            
        return p.toolkit.literal(json.dumps(data))

    def generate_json(self):
        return self.generate_output('json')
        
    def generate_jsonld(self):
        return self.generate_output('json-ld')

    def validator(self):
        # Validates that a URL is a good data.json file.
        if request.method == "POST" and "url" in request.POST and request.POST["url"].strip() != "":
            c.source_url = request.POST["url"]
            c.errors = []
            
            import urllib, json
            from datajsonvalidator import do_validation
            body = None
            try:
                body = json.load(urllib.urlopen(c.source_url))
            except IOError as e:
                c.errors.append(("Error Loading File", ["The address could not be loaded: " + unicode(e)]))
            except ValueError as e:
                c.errors.append(("Invalid JSON", ["The file does not meet basic JSON syntax requirements: " + unicode(e) + ". Try using JSONLint.com."]))
            except Exception as e:
                c.errors.append(("Internal Error", ["Something bad happened while trying to load and parse the file: " + unicode(e)]))
                
            if body:
                try:
                    do_validation(body, c.errors)
                except Exception as e:
                    c.errors.append(("Internal Error", ["Something bad happened: " + unicode(e)]))
                if len(c.errors) == 0:
                    c.errors.append(("No Errors", ["Great job!"]))
            
        return render('datajsonvalidator.html')

    def generate_pdl(self):
        # DWC this is a hack, as I couldn't get to the request parameters. For whatever reason, the multidict was always empty
        match = re.match(r"/organization/([-a-z0-9]+)/data.json", request.path)
        if match:
            # set content type (charset required or pylons throws an error)
            response.content_type = 'application/json; charset=UTF-8'

            # allow caching of response (e.g. by Apache)
            del response.headers["Cache-Control"]
            del response.headers["Pragma"]
            return make_pdl(match.group(1))
        return "Invalid organization id"

    def generate_edi(self):
        # DWC this is a hack, as I couldn't get to the request parameters. For whatever reason, the multidict was always empty
        match = re.match(r"/organization/([-a-z0-9]+)/edi.json", request.path)
        if match:
            # set content type (charset required or pylons throws an error)
            response.content_type = 'application/json; charset=UTF-8'

            # allow caching of response (e.g. by Apache)
            del response.headers["Cache-Control"]
            del response.headers["Pragma"]
            return make_edi(match.group(1))
        return "Invalid organization id"

def make_json():
    # Build the data.json file.
    packages = p.toolkit.get_action("current_package_list_with_resources")(None, {})
    output = []
    #Create data.json only using public and public-restricted datasets, datasets marked non-public are not exposed
    for pkg in packages:
        extras = dict([(x['key'], x['value']) for x in pkg['extras']])
        try:
            if not (re.match(r'[Nn]on-public', extras['public_access_level'])):
                datajson_entry = make_datajson_entry(pkg)
                if datajson_entry:
                    output.append(datajson_entry)
                else:
                    logger.warn("Dataset id=[%s], title=[%s] omitted", pkg.get('id', None), pkg.get('title', None))
        except KeyError:
            logger.warn("Dataset id=[%s], title=[%s] missing required 'public_access_level' field", pkg.get('id', None), pkg.get('title', None))
            pass
    return output

def make_edi(owner_org):
    #Error handler for creating error log
    stream = StringIO.StringIO()
    eh = logging.StreamHandler(stream)
    eh.setLevel(logging.WARN)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    eh.setFormatter(formatter)
    logger.addHandler(eh)

    # Build the data.json file.
    packages = get_all_group_packages(group_id=owner_org)
    output = []
    for pkg in packages:
        if pkg['owner_org'] == owner_org:
            datajson_entry = make_datajson_entry(pkg)
            if datajson_entry and is_valid(datajson_entry):
                output.append(datajson_entry)
            else:
                logger.warn("Dataset id=[%s], title=[%s] omitted", pkg.get('id', None), pkg.get('title', None))

    # Get the error log
    eh.flush()
    error = stream.getvalue()
    eh.close()
    logger.removeHandler(eh)
    stream.close()

    #return json.dumps(output)
    return write_zip(output, error, zip_name='edi')

def make_pdl(owner_org):
    #Error handler for creating error log
    stream = StringIO.StringIO()
    eh = logging.StreamHandler(stream)
    eh.setLevel(logging.WARN)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    eh.setFormatter(formatter)
    logger.addHandler(eh)


    # Build the data.json file.
    packages = get_all_group_packages(group_id=owner_org)

    output = []
    #Create data.json only using public datasets, datasets marked non-public are not exposed
    for pkg in packages:
        extras = dict([(x['key'], x['value']) for x in pkg['extras']])
        try:
            if pkg['owner_org'] == owner_org \
                and not (re.match(r'[Nn]on-public', extras['public_access_level'])):

                datajson_entry = make_datajson_entry(pkg)
                if datajson_entry and is_valid(datajson_entry):
                    output.append(datajson_entry)
                else:
                    logger.warn("Dataset id=[%s], title=[%s] omitted", pkg.get('id', None), pkg.get('title', None))

        except KeyError:
            logger.warn("Dataset id=[%s], title=['%s'] missing required 'public_access_level' field", pkg.get('id', None), pkg.get('title', None))
            pass

    # Get the error log
    eh.flush()
    error = stream.getvalue()
    eh.close()
    logger.removeHandler(eh)
    stream.close()

    #return json.dumps(output)
    return write_zip(output, error, zip_name='pdl')

def get_all_group_packages(group_id):
    """
    Gets all of the group packages, public or private, returning them as a list of CKAN's dictized packages.
    """
    result = []
    for pkg_rev in model.Group.get(group_id).packages(with_private=True, context={'user_is_admin':True}):
        result.append(model_dictize.package_dictize(pkg_rev, {'model': model}))

    return result

def is_valid(instance):
    """
    Validates a data.json entry against the project open data's JSON schema. Log a warning message on validation error
    """
    error = best_match(validator.iter_errors(instance))
    if error:
        logger.warn("Validation failed, best guess of error = %s", error)
        return False
    return True

def write_zip(data, error=None, zip_name='data'):
    """
    Data: a python object to write to the data.json
    Error: unicode string representing the content of the error log.
    zip_name: the name to use for the zip file
    """
    import zipfile

    o = StringIO.StringIO()
    zf = zipfile.ZipFile(o, mode='w')

    #Write the data file
    if data:
        zf.writestr('datajson.txt', json.dumps(data).encode('utf8'))

    #Write the error log
    if error:
        zf.writestr('errorlog.txt', error.encode('utf8'))

    zf.close()
    o.seek(0)

    binary = o.read()
    o.close()

    response.content_type = 'application/octet-stream'
    response.content_disposition = 'attachment; filename="%s.zip"' % zip_name

    return binary

