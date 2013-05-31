ckanext-datajson
================

A CKAN extension to generate the /data.json file required by the
U.S. Project Open Data guidelines (http://project-open-data.github.io/).

This module assumes metadata is stored in CKAN in the way we do it
on http://hub.healthdata.gov. If you're storing metadata under different
key names, you'll have to revise ckanext/datajson/plugin.py accordingly.

To install, activate your CKAN virtualenv and then install the module
in develop mode, which just puts the directory in your Python path.

	. path/to/pyenv/bin/activate
	python setup.py develop

Then in your CKAN .ini file, add ``datajson'' to your ckan.plugins line:

	ckan.plugins = (other plugins here...) datajson

If you're running CKAN via WSGI, we found a strange Python dependency
bug. It might only affect development environments. The fix was to
revise wsgi.py and add:

	import ckanext

before

	from paste.deploy import loadapp

If you're deploying inside Apache, some caching would be a good idea.
Enable the cache modules:

	a2enmod cache
	a2enmod disk_cache

And then in your Apache configuration add:

	CacheEnable disk /data.json
	CacheRoot /tmp/apache_cache
	CacheDefaultExpire 120
	CacheIgnoreCacheControl On
	CacheIgnoreNoLastMod On
	CacheStoreNoStore On

Then restart Apache and check out:

	http://yourdomain.com/data.json

Written by the HealthData.gov team.

As a work of the United States Government the files in this repository 
are in the public domain.

