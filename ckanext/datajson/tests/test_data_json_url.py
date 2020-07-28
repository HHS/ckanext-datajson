import logging
from nose.plugins.skip import SkipTest
from nose.tools import (assert_equal, assert_false, assert_in, assert_is_none,
                        assert_raises, assert_true)


log = logging.getLogger(__name__)


class TestDataJSONURL(object):
    """Test if we expose the /data.json URL"""

    def test_url(self):
        pass