ckanext-datajson
================

A CKAN extension to generate the /data.json file required by the
U.S. Project Open Data guidelines (http://project-open-data.github.io/).

This module assumes metadata is stored in CKAN in the way we do it
on http://hub.healthdata.gov. If you're storing metadata under different
key names, you'll have to revise ckanext/datajson/plugin.py accordingly.

Installation
------------

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

Then restart your server and check out:

	http://yourdomain.com/data.json

Caching The Response
--------------------

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

And be sure to create /tmp/apache_cache and make it writable by the Apache process.

Generating The File Off-Line
----------------------------

Generating this file is a little slow, so an alternative instead of caching is
to generate the file periodically (e.g. in a cron job). In that case, you'll want
to change the path that CKAN generates the file at to something *other* than /data.json.
In your CKAN .ini file, in the app:main section, add:

	ckanext.datajson.path = /internal/data.json

Now create a crontab file ("mycrontab") to download this URL to a file on disk
every ten minutes:

	0-59/10 * * * * wget -qO /path/to/static/data.json http://localhost/internal/data.json

And activate your crontab like so:

	crontab mycrontab

In Apache, we'll want to block outside access to the "internal" URL, and also
map the URL /data.json to the static file. In your httpd.conf, add:

	Alias /data.json /path/to/static/data.json
	
	<Location /internal/>
		Order deny,allow
		Allow from 127.0.0.1
		Deny from all
	</Location>

And then restart Apache. Wait for the cron job to run once, then check if
/data.json loads (and it should be fast!). Also double check that 
http://yourdomain.com/internal/data.json gives a 403 forbidden error when
accessed from some other location.

Credit / Copying
----------------

Written by the HealthData.gov team.

As a work of the United States Government the files in this repository 
are in the public domain.

