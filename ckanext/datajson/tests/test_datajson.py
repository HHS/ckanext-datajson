'''Tests for the datajson extension.

'''
import paste.fixture
import pylons.test

import ckan.model as model
import ckan.tests as tests
import ckan.plugins as plugins

class TestDatajsonPlugin(object):
    '''Tests for the datajson.plugin module.

    '''

    @classmethod
    def setup_class(cls):
        '''Nose runs this method once to setup our test class.'''

        # Make the Paste TestApp that we'll use to simulate HTTP requests to
        # CKAN.
        cls.app = paste.fixture.TestApp(pylons.test.pylonsapp)

        # Test code should use CKAN's plugins.load() function to load plugins
        # to be tested.
        #plugins.load('usmetadata')

    def setup(self):
        '''Nose runs this method before each test method in our test class.'''

        # Access CKAN's model directly (bad) to create a sysadmin user and save
        # it against self for all test methods to access.
        self.sysadmin = model.User(name='test_sysadmin', sysadmin=True)
        model.Session.add(self.sysadmin)
        model.Session.commit()
        model.Session.remove()

        self.org_dict = tests.call_action_api(self.app, 'organization_create', apikey=self.sysadmin.apikey, name='my_org_000')

        self.package_dict = tests.call_action_api(self.app, 'package_create', apikey=self.sysadmin.apikey,
                                             name='my_package_000',
                                             title='my package',
                                             notes='my package note',
                                             tag_string='my_package',
                                             ower_org = self.org_dict['id']
                                             )
        assert self.package_dict['name'] == 'my_package_000'
    def teardown(self):
        '''Nose runs this method after each test method in our test class.'''

        # Rebuild CKAN's database after each test method, so that each test
        # method runs with a clean slate.
        model.repo.rebuild_db()

    @classmethod
    def teardown_class(cls):
        '''Nose runs this method once after all the test methods in our class
        have been run.

        '''
        # We have to unload the plugin we loaded, so it doesn't affect any
        # tests that run after ours.
        plugins.unload('datajson')

    #test is dataset is getting created successfully
    def test_package_creation(self):
        package_dict = tests.call_action_api(self.app, 'package_create', apikey=self.sysadmin.apikey,
                                             name='my_package',
                                             title='my package',
                                             notes='my package notes',
                                             tag_string='my_package'
                                             )
        assert package_dict['name'] == 'my_package'