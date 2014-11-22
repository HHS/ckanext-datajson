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

    schema_path = os.path.join(os.path.dirname(__file__), 'schema', 'federal-v1.1', 'dataset.json')
    with open(schema_path, 'r') as file:
        schema = json.loads(file.read())
        return Draft4Validator(schema, format_checker=FormatChecker())

    logger.warn('Unable to create validator')
    return None


validator = get_validator()

try:
    from collections import OrderedDict  # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

import ckan.model

from package_to_pod import make_datajson_entry, get_facet_fields, make_datajson_catalog
from pod_jsonld import dataset_to_jsonld

# from build_enterprisedatajson import make_enterprisedatajson_entry


class DataJsonPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IConfigurer)
    p.implements(p.interfaces.IRoutes, inherit=True)
    p.implements(p.interfaces.IFacets)

    # IConfigurer
    
    def update_config(self, config):
        # Must use IConfigurer rather than IConfigurable because only IConfigurer
        # is called before after_map, in which we need the configuration directives
        # to know how to set the paths.

        # TODO commenting out enterprise data inventory for right now
        # DataJsonPlugin.route_edata_path = config.get("ckanext.enterprisedatajson.path", "/enterprisedata.json")
        DataJsonPlugin.route_enabled = config.get("ckanext.datajson.url_enabled", "True") == 'True'
        DataJsonPlugin.route_path = config.get("ckanext.datajson.path", "/data.json")
        DataJsonPlugin.route_hhs_path = config.get("ckanext.datajsonhhs.path", re.sub(r"\.json$", ".jsonhhs", DataJsonPlugin.route_path))
        EnterpriseDataJsonPlugin.route_path = config.get("ckanext.datajson.path", "/enterprisedata.json")
        DataJsonPlugin.route_ld_path = config.get("ckanext.datajsonld.path", re.sub(r"\.json$", ".jsonld", DataJsonPlugin.route_path))
        DataJsonPlugin.ld_id = config.get("ckanext.datajsonld.id", config.get("ckan.site_url"))
        DataJsonPlugin.ld_title = config.get("ckan.site_title", "Catalog")
        DataJsonPlugin.site_url = config.get("ckan.site_url")
        DataJsonPlugin.error_email_from = config.get("error_email_from")
        DataJsonPlugin.allow_harvester_deletion = config.get("ckanext.datajson.allow_harvester_deletion", True)
        DataJsonPlugin.email_to = config.get("email_to")
        DataJsonPlugin.default_contactpoint = config.get("ckanext.datajson.default_contactpoint")
        DataJsonPlugin.default_mbox = config.get("ckanext.datajson.default_mbox")
        DataJsonPlugin.default_keywords = config.get("ckanext.datajson.default_keywords")

        # Adds our local templates directory. It's smart. It knows it's
        # relative to the path of *this* file. Wow.
        p.toolkit.add_template_directory(config, "templates")

    # IRoutes

    def before_map(self, m):
        return m

    def after_map(self, m):
        if DataJsonPlugin.route_enabled:
            # /data.json and /data.jsonld (or other path as configured by user)
            m.connect('datajson', DataJsonPlugin.route_path, controller='ckanext.datajson.plugin:DataJsonController',
                      action='generate_json')
            # TODO commenting out enterprise data inventory for right now
            # m.connect('enterprisedatajson', DataJsonPlugin.route_edata_path, controller='ckanext.datajson.plugin:DataJsonController', action='generate_enterprise')
            #m.connect('datajsonld', DataJsonPlugin.route_ld_path, controller='ckanext.datajson.plugin:DataJsonController', action='generate_jsonld')
            m.connect('datajsonhhs', DataJsonPlugin.route_hhs_path, controller='ckanext.datajson.plugin:DataJsonController', action='generate_jsonhhs')

        # TODO DWC update action
        # /data/{org}/data.json
        m.connect('public_data_listing', '/organization/{org}/data.json',
                  controller='ckanext.datajson.plugin:DataJsonController', action='generate_pdl')

        # TODO DWC update action
        # /data/{org}/edi.json
        m.connect('enterprise_data_inventory', '/organization/{org}/edi.json',
                  controller='ckanext.datajson.plugin:DataJsonController', action='generate_edi')

        # /pod/validate
        # m.connect('datajsonvalidator', "/pod/validate", controller='ckanext.datajson.plugin:DataJsonController', action='validator')

        # /pod/data-listing
        m.connect('datajsonhtml', "/pod/data-catalog", controller='ckanext.datajson.plugin:DataJsonController', action='show_html_rendition')
        
        return m

    # IFacets
    
    def dataset_facets(self, facets, package_type):
        # Add any facets specified in package_to_pod.get_facet_fields() to the top
        # of the facet list, and then put the CKAN default facets below that.
        f = OrderedDict()
        f.update(get_facet_fields())
        f.update(facets)
        return f
    def group_facets(self, facets_dict, group_type, package_type):
        return facets_dict
    def organization_facets(self, facets_dict, organization_type, package_type):
        return facets_dict

class DataJsonController(BaseController):
    def generate_output(self, format):
        # set content type (charset required or pylons throws an error)
        response.content_type = 'application/json; charset=UTF-8'

        # allow caching of response (e.g. by Apache)
        del response.headers["Cache-Control"]
        del response.headers["Pragma"]

        # TODO special processing for enterprise
        # output
        if format == 'json-hhs':
            data = make_json_hhs()
        else:
            data = make_json()
        
        if format == 'json-ld':
            # Convert this to JSON-LD.
            data = OrderedDict([
                ("@context", OrderedDict([
                    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
                    ("dcterms", "http://purl.org/dc/terms/"),
                    ("dcat", "http://www.w3.org/ns/dcat#"),
                    ("foaf", "http://xmlns.com/foaf/0.1/"),
                    ("pod", "http://project-open-data.github.io/schema/2013-09-20_1.0#"),
                ])
                ),
                ("@id", DataJsonPlugin.ld_id),
                ("@type", "dcat:Catalog"),
                ("dcterms:title", DataJsonPlugin.ld_title),
                ("rdfs:label", DataJsonPlugin.ld_title),
                ("foaf:homepage", DataJsonPlugin.site_url),
                ("dcat:dataset", [dataset_to_jsonld(d) for d in data]),
            ])
            
        return p.toolkit.literal(json.dumps(data, indent=2))


    def generate_json(self):
        return self.generate_output('json')

    def generate_jsonhhs(self):
        return self.generate_output('json-hhs')
        
    def generate_jsonld(self):
        return self.generate_output('json-ld')

    def validator(self):
        # Validates that a URL is a good data.json file.
        if request.method == "POST" and "url" in request.POST and request.POST["url"].strip() != "":
            c.source_url = request.POST["url"]
            c.number_of_records = None
            c.errors = []

            import urllib, json
            from validator import do_validation
            body = None
            try:
                body = json.load(urllib.urlopen(c.source_url))
            except IOError as e:
                c.errors.append(("Error Loading File", ["The address could not be loaded: " + unicode(e)]))
            except ValueError as e:
                c.errors.append(("Invalid JSON", ["The file does not meet basic JSON syntax requirements: " + unicode(
                    e) + ". Try using JSONLint.com."]))
            except Exception as e:
                c.errors.append((
                "Internal Error", ["Something bad happened while trying to load and parse the file: " + unicode(e)]))

            if body:
                try:
                    do_validation(body, c.source_url, c.errors)
                    if type(body) == list:
                        c.number_of_records = len(body)
                except Exception as e:
                    c.errors.append(("Internal Error", ["Something bad happened: " + unicode(e)]))
                if len(c.errors) == 0:
                    c.errors.append(("No Errors", ["Great job!"]))

        return render('datajsonvalidator.html')

    def show_html_rendition(self):
        # Shows an HTML rendition of the data.json file. Requests the file live
        # from http://localhost/data.json.
            
        import urllib, json
        try:
            c.catalog_data = json.load(urllib.urlopen("http://localhost/data.json"))
        except:
            c.catalog_data = []
                
        c.catalog_data.sort(key = lambda x : x.get("modified"), reverse=True)

        return render('html_rendition.html')

def make_json_hhs():
    packages = p.toolkit.get_action("current_package_list_with_resources")(None, {})
    return [make_datajson_entry(pkg, DataJsonPlugin) for pkg in packages if pkg["type"] == "dataset" and pkg["author"] in ["Administration for Children and Families", "Administration for Community Living", "Agency for Healthcare Research and Quality", "Centers for Disease Control and Prevention", "Centers for Medicare & Medicaid Services", "Department of Health & Human Services", "Health Resources and Services Administration", "Indian Health Service", "National Cancer Institute", "National Institute on Drug Abuse", "National Institutes of Health", "National Library of Medicine", "Substance Abuse & Mental Health Services Administration", "U.S. Food and Drug Administration"] ]

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
    # Create data.json only using public and public-restricted datasets, datasets marked non-public are not exposed
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
            logger.warn("Dataset id=[%s], title=[%s] missing required 'public_access_level' field", pkg.get('id', None),
                        pkg.get('title', None))
            pass
    return output


def make_edi(owner_org):
    # Error handler for creating error log
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
    # Error handler for creating error log
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
            logger.warn("Dataset id=[%s], title=['%s'] missing required 'public_access_level' field",
                        pkg.get('id', None), pkg.get('title', None))
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
    for pkg_rev in model.Group.get(group_id).packages(with_private=True, context={'user_is_admin': True}):
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

    # Write the data file
    if data:
        zf.writestr('data.json', json.dumps(make_datajson_catalog(data), ensure_ascii=False).encode('utf8'))

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

