from datetime import datetime
import json
import logging
from urllib2 import URLError

import ckan.plugins as p
import ckanext.harvest.model as harvest_model
import ckanext.harvest.queue as queue
import mock_datajson_source
from ckan import model
from ckan.lib.munge import munge_title_to_name
from ckanext.datajson.harvester_datajson import DataJsonHarvester
from factories import HarvestJobObj, HarvestSourceObj
from mock import Mock, patch
from nose.tools import (assert_equal, assert_false, assert_in, assert_is_none,
                        assert_raises, assert_true)

try:
    from ckan.tests.helpers import reset_db, call_action
    from ckan.tests.factories import Organization, Group, Sysadmin
except ImportError:
    from ckan.new_tests.helpers import reset_db, call_action
    from ckan.new_tests.factories import Organization, Group, Sysadmin

log = logging.getLogger(__name__)


class TestDataJSONHarvester(object):

    @classmethod
    def setup_class(cls):
        log.info('Starting mock http server')
        cls.mock_port = 8961
        mock_datajson_source.serve(cls.mock_port)

    @classmethod
    def setup(cls):
        # Start data json sources server we can test harvesting against it
        reset_db()
        harvest_model.setup()
        cls.user = Sysadmin()
        
    def run_gather(self, url):
        self.source = HarvestSourceObj(url=url)
        self.job = HarvestJobObj(source=self.source)

        self.harvester = DataJsonHarvester()

        # gather stage
        log.info('GATHERING %s', url)
        obj_ids = self.harvester.gather_stage(self.job)
        log.info('job.gather_errors=%s', self.job.gather_errors)
        log.info('obj_ids=%s', obj_ids)
        if len(obj_ids) == 0:
            # nothing to see
            return

        self.harvest_objects = []
        for obj_id in obj_ids:
            harvest_object = harvest_model.HarvestObject.get(obj_id)
            log.info('ho guid=%s', harvest_object.guid)
            log.info('ho content=%s', harvest_object.content)
            self.harvest_objects.append(harvest_object)

        return obj_ids

    def run_fetch(self):
        # fetch stage

        for harvest_object in self.harvest_objects:
            log.info('FETCHING %s' % harvest_object.id)
            result = self.harvester.fetch_stage(harvest_object)

            log.info('ho errors=%s', harvest_object.errors)
            log.info('result 1=%s', result)
            if len(harvest_object.errors) > 0:
                self.errors = harvest_object.errors

    def run_import(self, objects=None):
        # import stage
        datasets = []
        
        # allow run just some objects
        if objects is None:
            # default is all objects in the right order
            objects = self.harvest_objects
        else:
            log.info('Import custom list {}'.format(objects))
        
        for harvest_object in objects:
            log.info('IMPORTING %s' % harvest_object.id)
            result = self.harvester.import_stage(harvest_object)
            
            log.info('ho errors 2=%s', harvest_object.errors)
            log.info('result 2=%s', result)
            
            if not result:
                log.error('Dataset not imported: {}. Errors: {}. Content: {}'.format(harvest_object.package_id, harvest_object.errors, harvest_object.content))

            if len(harvest_object.errors) > 0:
                self.errors = harvest_object.errors
                harvest_object.state = "ERROR"
            
            harvest_object.state = "COMPLETE"
            harvest_object.save()

            log.info('ho pkg id=%s', harvest_object.package_id)
            dataset = model.Package.get(harvest_object.package_id)
            if dataset:
                datasets.append(dataset)
                log.info('dataset name=%s', dataset.name)

        return datasets

    def run_source(self, url):
        self.run_gather(url)
        self.run_fetch()
        datasets = self.run_import()

        return datasets

    def test_datason_arm(self):
        url = 'http://127.0.0.1:%s/arm' % self.mock_port
        datasets = self.run_source(url=url)
        dataset = datasets[0]
        # assert_equal(first element on list
        expected_title = "NCEP GFS: vertical profiles of met quantities at standard pressures, at Barrow"
        assert_equal(dataset.title, expected_title)
        tags = [tag.name for tag in dataset.get_tags()]
        assert_in(munge_title_to_name("ORNL"), tags)
        assert_equal(len(dataset.resources), 1)

    def test_datason_usda(self):
        url = 'http://127.0.0.1:%s/usda' % self.mock_port
        datasets = self.run_source(url=url)
        dataset = datasets[0]
        expected_title = "Department of Agriculture Congressional Logs for Fiscal Year 2014"
        assert_equal(dataset.title, expected_title)
        tags = [tag.name for tag in dataset.get_tags()]
        assert_equal(len(dataset.resources), 1)
        assert_in(munge_title_to_name("Congressional Logs"), tags)

    def get_datasets_from_2_collection(self):
        url = 'http://127.0.0.1:%s/collection-2-parent-4-children.data.json' % self.mock_port
        self.run_gather(url=url)
        self.run_fetch()
        datasets = self.run_import()
        return datasets
        
    def test_harvesting_parent_child_2_collections(self):
        """ Test that we have the right parents in each case """
        
        datasets = self.get_datasets_from_2_collection()
        
        for dataset in datasets:
            parent_package_id = dataset.extras.get('collection_package_id', None)
            
            if dataset.title == 'Addressing AWOL':
                parent = model.Package.get(parent_package_id)
                # HEREX parent is None
                assert_equal(parent.title, 'Employee Relations Roundtables')
            elif dataset.title == 'Addressing AWOL 2':
                parent = model.Package.get(parent_package_id)
                assert_equal(parent.title, 'Employee Relations Roundtables 2') 

    def test_datajson_reserverd_word_as_title(self):
        url = 'http://127.0.0.1:%s/error-reserved-title' % self.mock_port
        self.run_source(url=url)
        errors = self.errors
        expected_error_stage = "Import"
        assert_equal(errors[0].stage, expected_error_stage)
        expected_error_message = "title: Search. That name cannot be used."
        assert_equal(errors[0].message, expected_error_message)

    def test_datajson_large_spatial(self):
        url = 'http://127.0.0.1:%s/error-large-spatial' % self.mock_port
        self.run_source(url=url)
        errors = self.errors
        expected_error_stage = "Import"
        assert_equal(errors[0].stage, expected_error_stage)
        expected_error_message = "spatial: Maximum allowed size is 32766. Actual size is 309643."
        assert_equal(errors[0].message, expected_error_message)

    def test_datajson_null_spatial(self):
        url = 'http://127.0.0.1:%s/null-spatial' % self.mock_port
        datasets = self.run_source(url=url)
        dataset = datasets[0]
        expected_title = "Sample Title NUll Spatial"
        assert_equal(dataset.title, expected_title)

    def test_datason_404(self):
        url = 'http://127.0.0.1:%s/404' % self.mock_port
        with assert_raises(URLError):
            self.run_source(url=url)

    def test_datason_500(self):
        url = 'http://127.0.0.1:%s/500' % self.mock_port
        with assert_raises(URLError):
            self.run_source(url=url)
