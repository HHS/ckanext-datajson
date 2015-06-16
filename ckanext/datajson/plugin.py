import logging
import StringIO
import json

import ckan.plugins as p
from ckan.lib.base import BaseController, render, c
from pylons import request, response
import re
import ckan.model as model
import ckan.lib.dictization.model_dictize as model_dictize
from jsonschema.exceptions import best_match
import build_datajson

logger = logging.getLogger('datajson')

try:
    from collections import OrderedDict  # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

# from build_datajson import make_datajson_entry, get_facet_fields

# from build_enterprisedatajson import make_enterprisedatajson_entry
from build_datajsonld import dataset_to_jsonld


class DataJsonPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IConfigurer)
    p.implements(p.interfaces.IRoutes, inherit=True)
    p.implements(p.interfaces.IFacets)

    # IConfigurer

    def update_config(self, config):
        # Must use IConfigurer rather than IConfigurable because only IConfigurer
        # is called before after_map, in which we need the configuration directives
        # to know how to set the paths.
        DataJsonPlugin.route_path = config.get("ckanext.datajson.path", "/data.json")
        DataJsonPlugin.route_ld_path = config.get("ckanext.datajsonld.path",
                                                  re.sub(r"\.json$", ".jsonld", DataJsonPlugin.route_path))
        DataJsonPlugin.ld_id = config.get("ckanext.datajsonld.id", config.get("ckan.site_url"))
        DataJsonPlugin.ld_title = config.get("ckan.site_title", "Catalog")
        DataJsonPlugin.site_url = config.get("ckan.site_url")

        # Adds our local templates directory. It's smart. It knows it's
        # relative to the path of *this* file. Wow.
        p.toolkit.add_template_directory(config, "templates")

    # IRoutes

    def before_map(self, m):
        return m

    def after_map(self, m):
        # /data.json and /data.jsonld (or other path as configured by user)
        m.connect('datajson', DataJsonPlugin.route_path, controller='ckanext.datajson.plugin:DataJsonController',
                  action='generate_json')
        m.connect('datajsonld', DataJsonPlugin.route_ld_path, controller='ckanext.datajson.plugin:DataJsonController',
                  action='generate_jsonld')

        # /pod/validate
        m.connect('datajsonvalidator', "/pod/validate", controller='ckanext.datajson.plugin:DataJsonController',
                  action='validator')

        # /pod/data-listing
        m.connect('datajsonhtml', "/pod/data-catalog", controller='ckanext.datajson.plugin:DataJsonController',
                  action='show_html_rendition')

        return m

    # IFacets

    def dataset_facets(self, facets, package_type):
        # Add any facets specified in build_datajson.get_facet_fields() to the top
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
    def generate_output(self, fmt):
        # set content type (charset required or pylons throws an error)
        response.content_type = 'application/json; charset=UTF-8'

        # allow caching of response (e.g. by Apache)
        del response.headers["Cache-Control"]
        del response.headers["Pragma"]

        # output
        data = self.make_json()

        if fmt == 'json-ld':
            # Convert this to JSON-LD.
            data = OrderedDict([
                ("@context", OrderedDict([
                    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
                    ("dcterms", "http://purl.org/dc/terms/"),
                    ("dcat", "http://www.w3.org/ns/dcat#"),
                    ("foaf", "http://xmlns.com/foaf/0.1/"),
                    ("pod", "http://project-open-data.github.io/schema/2013-09-20_1.0#"),
                ])),
                ("@id", DataJsonPlugin.ld_id),
                ("@type", "dcat:Catalog"),
                ("dcterms:title", DataJsonPlugin.ld_title),
                ("rdfs:label", DataJsonPlugin.ld_title),
                ("foaf:homepage", DataJsonPlugin.site_url),
                ("dcat:dataset", [dataset_to_jsonld(d) for d in data]),
            ])

        return p.toolkit.literal(json.dumps(data, indent=2))

    # def make_json(self):
    #     # Build the data.json file.
    #     packages = p.toolkit.get_action("current_package_list_with_resources")(None, {})
    #     return [make_datajson_entry(pkg) for pkg in packages if pkg["type"] == "dataset"]

    def generate_json(self):
        return self.generate_output('json')

    def generate_jsonld(self):
        return self.generate_output('json-ld')

    def validator(self):
        # Validates that a URL is a good data.json file.
        if request.method == "POST" and "url" in request.POST and request.POST["url"].strip() != "":
            c.source_url = request.POST["url"]
            c.errors = []

            import urllib
            import json
            from datajsonvalidator import do_validation

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
                    "Internal Error",
                    ["Something bad happened while trying to load and parse the file: " + unicode(e)]))

            if body:
                try:
                    do_validation(body, c.source_url, c.errors)
                except Exception as e:
                    c.errors.append(("Internal Error", ["Something bad happened: " + unicode(e)]))
                if len(c.errors) == 0:
                    c.errors.append(("No Errors", ["Great job!"]))

        return render('datajsonvalidator.html')

    def show_html_rendition(self):
        # Shows an HTML rendition of the data.json file. Requests the file live
        # from http://localhost/data.json.

        import urllib
        import json

        try:
            c.catalog_data = json.load(urllib.urlopen("http://localhost/data.json"))
        except Exception as e:
            c.catalog_data = []

        c.catalog_data.sort(key=lambda x: x.get("modified"), reverse=True)

        return render('html_rendition.html')


class JsonExportPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IConfigurer)
    p.implements(p.interfaces.IRoutes, inherit=True)

    def update_config(self, config):
        # Must use IConfigurer rather than IConfigurable because only IConfigurer
        # is called before after_map, in which we need the configuration directives
        # to know how to set the paths.

        # TODO commenting out enterprise data inventory for right now
        # JsonExportPlugin.route_edata_path = config.get("ckanext.enterprisedatajson.path", "/enterprisedata.json")
        JsonExportPlugin.route_enabled = config.get("ckanext.datajson.url_enabled", "True") == 'True'
        JsonExportPlugin.route_path = config.get("ckanext.datajson.path", "/data.json")
        JsonExportPlugin.route_ld_path = config.get("ckanext.datajsonld.path",
                                                    re.sub(r"\.json$", ".jsonld", JsonExportPlugin.route_path))
        JsonExportPlugin.ld_id = config.get("ckanext.datajsonld.id", config.get("ckan.site_url"))
        JsonExportPlugin.ld_title = config.get("ckan.site_title", "Catalog")
        JsonExportPlugin.site_url = config.get("ckan.site_url")

        # Adds our local templates directory. It's smart. It knows it's
        # relative to the path of *this* file. Wow.
        p.toolkit.add_template_directory(config, "templates")

    def before_map(self, m):
        return m

    def after_map(self, m):
        if JsonExportPlugin.route_enabled:
            # /data.json and /data.jsonld (or other path as configured by user)
            m.connect('datajson_export', JsonExportPlugin.route_path,
                      controller='ckanext.datajson.plugin:JsonExportController',
                      action='generate_json')
            # TODO commenting out enterprise data inventory for right now
            # m.connect('enterprisedatajson', JsonExportPlugin.route_edata_path,
            # controller='ckanext.datajson.plugin:JsonExportController', action='generate_enterprise')

            # m.connect('datajsonld', JsonExportPlugin.route_ld_path,
            # controller='ckanext.datajson.plugin:JsonExportController', action='generate_jsonld')

        # TODO DWC update action
        # /data/{org}/data.json
        m.connect('public_data_listing', '/organization/{org}/data.json',
                  controller='ckanext.datajson.plugin:JsonExportController', action='generate_pdl')

        # TODO DWC update action
        # /data/{org}/edi.json
        m.connect('enterprise_data_inventory', '/organization/{org}/edi.json',
                  controller='ckanext.datajson.plugin:JsonExportController', action='generate_edi')

        # TODO DWC update action
        # /data/{org}/edi.json
        m.connect('enterprise_data_inventory', '/organization/{org}/draft.json',
                  controller='ckanext.datajson.plugin:JsonExportController', action='generate_draft')

        # /pod/validate
        # m.connect('datajsonvalidator', "/pod/validate",
        # controller='ckanext.datajson.plugin:JsonExportController', action='validator')

        return m


class JsonExportController(BaseController):
    _errors_json = []

    def generate_output(self, fmt):
        # set content type (charset required or pylons throws an error)
        response.content_type = 'application/json; charset=UTF-8'

        # allow caching of response (e.g. by Apache)
        del response.headers["Cache-Control"]
        del response.headers["Pragma"]

        # TODO special processing for enterprise
        # output
        data = self.make_json()

        if fmt == 'json-ld':
            # Convert this to JSON-LD.
            data = OrderedDict([
                ("@context", OrderedDict([
                    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
                    ("dcterms", "http://purl.org/dc/terms/"),
                    ("dcat", "http://www.w3.org/ns/dcat#"),
                    ("foaf", "http://xmlns.com/foaf/0.1/"),
                ])),
                ("@id", JsonExportPlugin.ld_id),
                ("@type", "dcat:Catalog"),
                ("dcterms:title", JsonExportPlugin.ld_title),
                ("rdfs:label", JsonExportPlugin.ld_title),
                ("foaf:homepage", JsonExportPlugin.site_url),
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

            import urllib
            import json
            from datajsonvalidator import do_validation

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
                    "Internal Error",
                    ["Something bad happened while trying to load and parse the file: " + unicode(e)]))

            if body:
                try:
                    do_validation(body, c.errors)
                except Exception as e:
                    c.errors.append(("Internal Error", ["Something bad happened: " + unicode(e)]))
                if len(c.errors) == 0:
                    c.errors.append(("No Errors", ["Great job!"]))

        return render('datajsonvalidator.html')

    def generate_pdl(self):
        # DWC this is a hack, as I couldn't get to the request parameters.
        #  For whatever reason, the multidict was always empty
        match = re.match(r"/organization/([-a-z0-9]+)/data.json", request.path)

        # If user is not editor or admin of the organization then don't allow pdl download
        if p.toolkit.check_access('package_create', {'model': model, 'user': c.user}, {'owner_org': match.group(1)}):
            if match:
                # set content type (charset required or pylons throws an error)
                response.content_type = 'application/json; charset=UTF-8'

                # allow caching of response (e.g. by Apache)
                del response.headers["Cache-Control"]
                del response.headers["Pragma"]
                return self.make_pdl(match.group(1))
        return "Invalid organization id"

    def generate_edi(self):
        # DWC this is a hack, as I couldn't get to the request parameters.
        # For whatever reason, the multidict was always empty
        match = re.match(r"/organization/([-a-z0-9]+)/edi.json", request.path)

        # If user is not editor or admin of the organization then don't allow edi download
        if p.toolkit.check_access('package_create', {'model': model, 'user': c.user}, {'owner_org': match.group(1)}):
            if match:
                # set content type (charset required or pylons throws an error)
                response.content_type = 'application/json; charset=UTF-8'

                # allow caching of response (e.g. by Apache)
                del response.headers["Cache-Control"]
                del response.headers["Pragma"]
                return self.make_edi(match.group(1))
        return "Invalid organization id"

    def generate_draft(self):
        # DWC this is a hack, as I couldn't get to the request parameters.
        # For whatever reason, the multidict was always empty
        match = re.match(r"/organization/([-a-z0-9]+)/draft.json", request.path)

        # If user is not editor or admin of the organization then don't allow edi download
        if p.toolkit.check_access('package_create', {'model': model, 'user': c.user}, {'owner_org': match.group(1)}):
            if match:
                # set content type (charset required or pylons throws an error)
                response.content_type = 'application/json; charset=UTF-8'

                # allow caching of response (e.g. by Apache)
                del response.headers["Cache-Control"]
                del response.headers["Pragma"]
                return self.make_draft(match.group(1))
        return "Invalid organization id"

    def make_json(self):
        # Build the data.json file.
        packages = p.toolkit.get_action("current_package_list_with_resources")(None, {})
        output = []
        seen_identifiers = set()
        json_export_map = self.get_export_map_json()

        # Create data.json only using public and public-restricted datasets, datasets marked non-public are not exposed
        for pkg in packages:
            extras = dict([(x['key'], x['value']) for x in pkg['extras']])
            try:
                if not (re.match(r'[Nn]on-public', extras['public_access_level'])):
                    datajson_entry = build_datajson.JsonExportBuilder.make_datajson_export_entry(pkg, json_export_map, seen_identifiers)
                    if datajson_entry:
                        output.append(datajson_entry)
                    else:
                        publisher = self.detect_publisher(extras)
                        logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted\n", pkg.get('id', None),
                                    pkg.get('title', None), publisher)
            except KeyError:
                publisher = self.detect_publisher(extras)

                logger.warn(
                    "Dataset id=[%s], title=[%s], organization=[%s] missing required 'public_access_level' field",
                    pkg.get('id', None),
                    pkg.get('title', None),
                    publisher)

                errors = ['Missing Required Field', ['public_access_level']]

                self._errors_json.append(OrderedDict([
                    ('id', pkg.get('id')),
                    ('name', pkg.get('name')),
                    ('title', pkg.get('title')),
                    ('organization', publisher),
                    ('errors', errors),
                ]))
                pass
        return output

    def make_draft(self, owner_org):
        # Error handler for creating error log
        stream = StringIO.StringIO()
        eh = logging.StreamHandler(stream)
        eh.setLevel(logging.WARN)
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        eh.setFormatter(formatter)
        logger.addHandler(eh)

        # Build the data.json file.
        packages = self.get_packages(owner_org)
        json_export_map = self.get_export_map_json()

        errors_json = []

        output = []
        seen_identifiers = set()

        for pkg in packages:
            extras = dict([(x['key'], x['value']) for x in pkg['extras']])
            if 'publishing_status' not in extras.keys() or extras['publishing_status'] != 'Draft':
                continue
            datajson_entry = build_datajson.JsonExportBuilder.make_datajson_export_entry(pkg, json_export_map, seen_identifiers)
            if 'errors' in datajson_entry.keys():
                errors_json.append(datajson_entry)
                datajson_entry = None
            if datajson_entry and self.is_valid(datajson_entry):
                output.append(datajson_entry)
            else:
                publisher = self.detect_publisher(extras)
                logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted\n", pkg.get('id', None),
                            pkg.get('title', None), publisher)

        # Get the error log
        eh.flush()
        error = stream.getvalue()
        eh.close()
        logger.removeHandler(eh)
        stream.close()

        # return json.dumps(output)
        return self.write_zip(output, error, errors_json, zip_name='draft')

    @staticmethod
    def detect_publisher(extras):
        publisher = None

        if 'publisher' in extras and extras['publisher']:
            publisher = build_datajson.JsonExportBuilder.strip_if_string(extras['publisher'])

        for i in range(1, 6):
            key = 'publisher_' + str(i)
            if key in extras and extras[key] and build_datajson.JsonExportBuilder.strip_if_string(extras[key]):
                publisher = build_datajson.JsonExportBuilder.strip_if_string(extras[key])
        return publisher

    @staticmethod
    def get_export_map_json():
        # Reading json export map from file
        import os
        map_path = os.path.join(os.path.dirname(__file__), 'export_map', 'export.map.json')

        with open(map_path, 'r') as export_map_json:
            json_export_map = json.load(export_map_json)

        return json_export_map

    def make_edi(self, owner_org):
        # Error handler for creating error log
        stream = StringIO.StringIO()
        eh = logging.StreamHandler(stream)
        eh.setLevel(logging.WARN)
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        eh.setFormatter(formatter)
        logger.addHandler(eh)

        # Build the data.json file.
        packages = self.get_packages(owner_org)

        json_export_map = self.get_export_map_json()

        output = []
        errors_json = []
        seen_identifiers = set()

        for pkg in packages:
            extras = dict([(x['key'], x['value']) for x in pkg['extras']])
            if 'publishing_status' in extras.keys() and extras['publishing_status'] == 'Draft':
                continue
            datajson_entry = build_datajson.JsonExportBuilder.make_datajson_export_entry(pkg, json_export_map, seen_identifiers)
            if 'errors' in datajson_entry.keys():
                errors_json.append(datajson_entry)
                datajson_entry = None
            if datajson_entry and self.is_valid(datajson_entry):
                output.append(datajson_entry)
            else:
                publisher = self.detect_publisher(extras)
                logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted\n", pkg.get('id', None),
                            pkg.get('title', None), publisher)

        # Get the error log
        eh.flush()
        error = stream.getvalue()
        eh.close()
        logger.removeHandler(eh)
        stream.close()

        # return json.dumps(output)
        return self.write_zip(output, error, errors_json, zip_name='edi')

    def make_pdl(self, owner_org):
        # Error handler for creating error log
        stream = StringIO.StringIO()
        eh = logging.StreamHandler(stream)
        eh.setLevel(logging.WARN)
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        eh.setFormatter(formatter)
        logger.addHandler(eh)

        # Build the data.json file.
        packages = self.get_packages(owner_org)

        output = []
        errors_json = []
        seen_identifiers = set()

        json_export_map = self.get_export_map_json()

        # Create data.json only using public datasets, datasets marked non-public are not exposed
        for pkg in packages:
            extras = dict([(x['key'], x['value']) for x in pkg['extras']])
            if 'publishing_status' in extras.keys() and extras['publishing_status'] == 'Draft':
                continue
            try:
                if re.match(r'[Nn]on-public', extras['public_access_level']):
                    continue
                datajson_entry = build_datajson.JsonExportBuilder.make_datajson_export_entry(pkg, json_export_map, seen_identifiers)
                if 'errors' in datajson_entry.keys():
                    errors_json.append(datajson_entry)
                    datajson_entry = None
                if datajson_entry and self.is_valid(datajson_entry):
                    output.append(datajson_entry)
                else:
                    publisher = self.detect_publisher(extras)
                    logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted\n", pkg.get('id', None),
                                pkg.get('title', None), publisher)

            except KeyError:
                publisher = self.detect_publisher(extras)

                logger.warn(
                    "Dataset id=[%s], title=['%s'], organization=['%s'] missing required 'public_access_level' field",
                    pkg.get('id', None), pkg.get('title', None), publisher)

                errors = ['Missing Required Field', ['public_access_level']]

                self._errors_json.append(OrderedDict([
                    ('id', pkg.get('id')),
                    ('name', pkg.get('name')),
                    ('title', pkg.get('title')),
                    ('organization', publisher),
                    ('errors', errors),
                ]))
                pass

        # Get the error log
        eh.flush()
        error = stream.getvalue()
        eh.close()
        logger.removeHandler(eh)
        stream.close()

        # return json.dumps(output)
        return self.write_zip(output, error, errors_json, zip_name='pdl')

    def get_packages(self, owner_org):
        # Build the data.json file.
        packages = self.get_all_group_packages(group_id=owner_org)
        # get packages for sub-agencies.
        sub_agency = model.Group.get(owner_org)
        if 'sub-agencies' in sub_agency.extras.col.target \
                and sub_agency.extras.col.target['sub-agencies'].state == 'active':
            sub_agencies = sub_agency.extras.col.target['sub-agencies'].value
            sub_agencies_list = sub_agencies.split(",")
            for sub in sub_agencies_list:
                sub_packages = self.get_all_group_packages(group_id=sub)
                for sub_package in sub_packages:
                    packages.append(sub_package)

        return packages

    def get_all_group_packages(self, group_id):
        """
        Gets all of the group packages, public or private, returning them as a list of CKAN's dictized packages.
        """
        result = []
        for pkg_rev in model.Group.get(group_id).packages(with_private=True, context={'user_is_admin': True}):
            result.append(model_dictize.package_dictize(pkg_rev, {'model': model}))

        return result

    def is_valid(self, instance):
        """
        Validates a data.json entry against the project open data's JSON schema.
        Log a warning message on validation error
        """
        error = best_match(validator.iter_errors(instance))
        if error:
            logger.warn("Validation failed, best guess of error = %s", error)
            return False
        return True

    def write_zip(self, data, error=None, errors_json=None, zip_name='data'):
        """
        Data: a python object to write to the data.json
        Error: unicode string representing the content of the error log.
        zip_name: the name to use for the zip file
        """
        import zipfile

        o = StringIO.StringIO()
        zf = zipfile.ZipFile(o, mode='w')

        data_file_name = 'data.json'
        if 'draft' == zip_name:
            data_file_name = 'draft_data.json'

        # Write the data file
        if data:
            zf.writestr(data_file_name,
                        json.dumps(build_datajson.JsonExportBuilder.make_datajson_export_catalog(data), ensure_ascii=False).encode(
                            'utf8'))
        # Write empty.json if nothing to return
        else:
            zf.writestr('empty.json', '')

        if self._errors_json:
            if errors_json:
                errors_json += self._errors_json
            else:
                errors_json = self._errors_json

        # Errors in json format
        if errors_json:
            zf.writestr('errors.json', json.dumps(errors_json).encode('utf8'))

        # Write the error log
        if error:
            zf.writestr('errorlog.txt', error.encode('utf8'))

        zf.close()
        o.seek(0)

        binary = o.read()
        o.close()

        response.content_type = 'application/octet-stream'
        response.content_disposition = 'attachment; filename="%s.zip"' % zip_name

        return binary


def get_validator():
    import os
    from jsonschema import Draft4Validator, FormatChecker

    schema_path = os.path.join(os.path.dirname(__file__), 'pod_schema', 'federal-v1.1', 'dataset.json')
    with open(schema_path, 'r') as schema:
        schema = json.loads(schema.read())
        return Draft4Validator(schema, format_checker=FormatChecker())


validator = get_validator()
