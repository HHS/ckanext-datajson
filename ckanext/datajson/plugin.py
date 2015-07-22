import logging
import StringIO
import json
import sys

import ckan.plugins as p
from ckan.lib.base import BaseController, render, c
from pylons import request, response
import re
import ckan.model as model
import os
import ckan.lib.dictization.model_dictize as model_dictize

from jsonschema.exceptions import best_match

from helpers import get_export_map_json, detect_publisher, get_validator
from package2pod import Package2Pod

logger = logging.getLogger('datajson')
draft4validator = get_validator()

try:
    from collections import OrderedDict  # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

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
        DataJsonPlugin.route_enabled = config.get("ckanext.datajson.url_enabled", "True") == 'True'
        DataJsonPlugin.route_path = config.get("ckanext.datajson.path", "/data.json")
        DataJsonPlugin.route_ld_path = config.get("ckanext.datajsonld.path",
                                                  re.sub(r"\.json$", ".jsonld", DataJsonPlugin.route_path))
        DataJsonPlugin.ld_id = config.get("ckanext.datajsonld.id", config.get("ckan.site_url"))
        DataJsonPlugin.ld_title = config.get("ckan.site_title", "Catalog")
        DataJsonPlugin.site_url = config.get("ckan.site_url")

        DataJsonPlugin.edi_pdl_enabled = config.get("ckanext.datajson.edi_pdl_enabled", "False") == 'True'

        # Adds our local templates directory. It's smart. It knows it's
        # relative to the path of *this* file. Wow.
        p.toolkit.add_template_directory(config, "templates")

    def before_map(self, m):
        return m

    def after_map(self, m):
        if DataJsonPlugin.route_enabled:
            # /data.json and /data.jsonld (or other path as configured by user)
            m.connect('datajson_export', DataJsonPlugin.route_path,
                      controller='ckanext.datajson.plugin:DataJsonController',
                      action='generate_json')
            # TODO commenting out enterprise data inventory for right now
            # m.connect('enterprisedatajson', DataJsonPlugin.route_edata_path,
            # controller='ckanext.datajson.plugin:DataJsonController', action='generate_enterprise')

            # m.connect('datajsonld', DataJsonPlugin.route_ld_path,
            # controller='ckanext.datajson.plugin:DataJsonController', action='generate_jsonld')

        if DataJsonPlugin.edi_pdl_enabled:
            m.connect('public_data_listing', '/organization/{org}/pdl.json',
                      controller='ckanext.datajson.plugin:DataJsonController', action='generate_pdl')

            m.connect('enterprise_data_inventory', '/organization/{org}/edi.json',
                      controller='ckanext.datajson.plugin:DataJsonController', action='generate_edi')

            m.connect('enterprise_data_inventory', '/organization/{org}/draft.json',
                      controller='ckanext.datajson.plugin:DataJsonController', action='generate_draft')

        # /pod/validate
        m.connect('datajsonvalidator', "/pod/validate",
                  controller='ckanext.datajson.plugin:DataJsonController', action='validator')

        return m


class DataJsonController(BaseController):
    _errors_json = []

    def generate_json(self):
        return self.generate_output('json')

    def generate_jsonld(self):
        return self.generate_output('json-ld')

    def generate_pdl(self):
        return self.generate('pdl')

    def generate_edi(self):
        return self.generate('edi')

    def generate_draft(self):
        return self.generate('draft')

    def generate(self, export_type='datajson'):
        if 'datajson' == export_type:
            return self.generate_output('json')
        if export_type in ['draft', 'pdl', 'edi']:
            # DWC this is a hack, as I couldn't get to the request parameters.
            # For whatever reason, the multidict was always empty
            match = re.match(r"/organization/([-a-z0-9]+)/" + export_type + r".json", request.path)

            # If user is not editor or admin of the organization then don't allow edi download
            if p.toolkit.check_access('package_create', {'model': model, 'user': c.user},
                                      {'owner_org': match.group(1)}):
                if match:
                    # set content type (charset required or pylons throws an error)
                    response.content_type = 'application/json; charset=UTF-8'

                    # allow caching of response (e.g. by Apache)
                    del response.headers["Cache-Control"]
                    del response.headers["Pragma"]
                    return self.make_json(export_type, match.group(1))
            return "Invalid organization id"
        return "Invalid type"

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
                ("@id", DataJsonPlugin.ld_id),
                ("@type", "dcat:Catalog"),
                ("dcterms:title", DataJsonPlugin.ld_title),
                ("rdfs:label", DataJsonPlugin.ld_title),
                ("foaf:homepage", DataJsonPlugin.site_url),
                ("dcat:dataset", [dataset_to_jsonld(d) for d in data.get('dataset')]),
            ])

        return p.toolkit.literal(json.dumps(data, indent=2))

    def make_json(self, export_type='datajson', owner_org=None):
        # Error handler for creating error log
        stream = StringIO.StringIO()
        eh = logging.StreamHandler(stream)
        eh.setLevel(logging.WARN)
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        eh.setFormatter(formatter)
        logger.addHandler(eh)

        data = ''
        output = []
        errors_json = []
        Package2Pod.seen_identifiers = set()

        # Skip validation if exporting whole catalog /data.json
        validation_required = 'datajson' != export_type

        try:
            # Build the data.json file.
            if owner_org:
                packages = self.get_packages(owner_org)
            else:
                # temporarily limit number of records
                packages = p.toolkit.get_action("current_package_list_with_resources")(None, {'limit': 50})
                # packages = p.toolkit.get_action("current_package_list_with_resources")(None, {})

            json_export_map = get_export_map_json('export.map.json')

            if json_export_map:
                for pkg in packages:
                    # logger.error('package: %s', json.dumps(pkg))
                    # logger.debug("processing %s" % (pkg.get('title')))
                    extras = dict([(x['key'], x['value']) for x in pkg['extras']])

                    # edi = all non-draft datasets (public + private)
                    # pdl = public-only, non-draft datasets
                    if export_type in ['edi', 'pdl']:
                        if 'Draft' == extras.get('publishing_status'):
                            publisher = detect_publisher(extras)
                            logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted (%s)\n",
                                        pkg.get('id', None), pkg.get('title', None), publisher,
                                        'publishing_status: Draft')
                            continue
                        if 'pdl' == export_type and re.match(r'[Nn]on-public', extras['public_access_level']):
                            continue
                    # draft = all draft-only datasets
                    elif 'draft' == export_type:
                        if 'publishing_status' not in extras.keys() or extras['publishing_status'] != 'Draft':
                            continue

                    datajson_entry = Package2Pod.convert_package(pkg, json_export_map, validation_required)
                    errors = None
                    if 'errors' in datajson_entry.keys():
                        errors_json.append(datajson_entry)
                        errors = datajson_entry.get('errors')
                        datajson_entry = None

                    if datajson_entry and (not validation_required or self.is_valid(datajson_entry)):
                        # logger.debug("writing to json: %s" % (pkg.get('title')))
                        output.append(datajson_entry)
                    else:
                        publisher = detect_publisher(extras)
                        if errors:
                            logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted, reason below:\n\t%s\n",
                                        pkg.get('id', None), pkg.get('title', None), publisher, errors)
                        else:
                            logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted, reason above.\n",
                                        pkg.get('id', None), pkg.get('title', None), publisher)

                data = Package2Pod.wrap_json_catalog(output, json_export_map)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("%s : %s : %s : %s", exc_type, filename, exc_tb.tb_lineno, unicode(e))

        # Get the error log
        eh.flush()
        error = stream.getvalue()
        eh.close()
        logger.removeHandler(eh)
        stream.close()

        # Skip compression if we export whole /data.json catalog
        if 'datajson' == export_type:
            return data

        return self.write_zip(data, error, errors_json, zip_name=export_type)

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
        error = best_match(draft4validator.iter_errors(instance))
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
            zf.writestr(data_file_name, json.dumps(data, ensure_ascii=False).encode('utf8'))

        # Write empty.json if nothing to return
        else:
            # logger.debug('no data to write')
            zf.writestr('empty.json', '')

        if self._errors_json:
            if errors_json:
                errors_json += self._errors_json
            else:
                errors_json = self._errors_json

        # Errors in json format
        if errors_json:
            # logger.debug('writing errors.json')
            zf.writestr('errors.json', json.dumps(errors_json).encode('utf8'))

        # Write the error log
        if error:
            # logger.debug('writing errorlog.txt')
            zf.writestr('errorlog.txt', error.encode('utf8'))

        zf.close()
        o.seek(0)

        binary = o.read()
        o.close()

        response.content_type = 'application/octet-stream'
        response.content_disposition = 'attachment; filename="%s.zip"' % zip_name

        return binary

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
