ckanext-datajson
================

A CKAN extension to generate the /data.json file and to harvest data
sources from a remote /data.json file according to the U.S. Project
Open Data metadata specification (http://project-open-data.github.io/).

This plugin creates a new view at /data.json (or other configurable
path) that outputs the contents of the data catalog in the Project
Open Data JSON metadata format. It also creates a view at /data.jsonld
which outputs the same in JSON-LD format.

The plugin also provides a harvester to import datasets from other
remote /data.json files. See below for setup instructions.

And the plugin also provides a new view to validate /data.json files
at http://ckanhostname/pod/validate.

This module assumes metadata is stored in CKAN in the way we do it
on http://hub.healthdata.gov. If you're storing metadata under different
key names, you'll have to revise ckanext/datajson/plugin.py accordingly.

Installation
------------

To install, activate your CKAN virtualenv, install dependencies, and
install the module in develop mode, which just puts the directory in your
Python path.

	. path/to/pyenv/bin/activate
	pip install -r pip-requirements.txt
	python setup.py develop

Then in your CKAN .ini file, add ``datajson'' to your ckan.plugins line:

	ckan.plugins = (other plugins here...) datajson

That's the plugin for /data.json output. To make the harvester available,
also add:

	ckan.plugins = (other plugins here...) harvest datajson_harvest

If you're running CKAN via WSGI, we found a strange Python dependency
bug. It might only affect development environments. The fix was to
revise wsgi.py and add:

	import ckanext

before

	from paste.deploy import loadapp

Then restart your server and check out:

	http://yourdomain.com/data.json
	   and
	http://yourdomain.com/data.jsonld
	   and
	http://yourdomain.com/pod/validate	
	
Caching The Response
--------------------

If you're deploying inside Apache, some caching would be a good idea
because generating the /data.json file can take a good few moments.
Enable the cache modules:

	a2enmod cache
	a2enmod disk_cache

And then in your Apache configuration add:

	CacheEnable disk /data.json
	CacheRoot /tmp/apache_cache
	CacheDefaultExpire 120
	CacheMaxFileSize 50000000
	CacheIgnoreCacheControl On
	CacheIgnoreNoLastMod On
	CacheStoreNoStore On

And be sure to create /tmp/apache_cache and make it writable by the Apache process.

Generating /data.json Off-Line
------------------------------

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

Options
-------

You can customize the URL that generates the data.json output:

	ckanext.datajson.path = /data.json
	ckanext.datajsonld.path = /data.jsonld
	ckanext.datajsonld.id = http://www.youragency.gov/data.json

You can enable or disable the Data.json output by setting

    ckanext.datajson.url_enabled = False

If ckanext.datajsonld.path is omitted, it defaults to replacing ".json" in your
ckanext.datajson.path path with ".jsonld", so it probably won't need to be
specified.

The option ckanext.datajsonld.id is the @id value used to identify the data
catalog itself. If not given, it defaults to ckan.site_url.

You can also set some default values for datasets that are missing required
fields. These are possible:

	ckanext.datajson.default_contactpoint = Health Data Initiative
	ckanext.datajson.default_mbox = Healthdata@example.hhs.gov
	ckanext.datajson.default_keywords = health

The Harvester
-------------

To use the data.json harvester, you'll also need to set up the CKAN harvester
extension. See the CKAN harvester README at https://github.com/okfn/ckanext-harvest
for how to do that. You'll set some configuration variables and then initialize the
CKAN harvester plugin using:

	paster --plugin=ckanext-harvest harvester initdb --config=/path/to/ckan.ini

Now you can set up a new DataJson harvester by visiting:

	http://yourdomain.com/harvest

And when configuring the data source, just choose "/data.json" as the source type.

**The next paragraph assumes you're using my fork of the CKAN harvest extension
at https://github.com/JoshData/ckanext-harvest**

In the configuration field, you can put a YAML string containing defaults for fields
that may not be set in the source data.json files, e.g. enter something like this:

	defaults:
	  Agency: Department of Health & Human Services
	  author: Substance Abuse & Mental Health Services Administration
	  author_id: http://healthdata.gov/id/agency/samhsa

The keys `title`, `notes`, `author`, and `url` are stored in the corresponding CKAN
package fields. `tags`, which must be an array of strings, is stored as CKAN package
tags. All other keys are stored in package extras (i.e. the Additional Info of a
package).

You may also override values found in the harvested file with fixed values set in the configuration like so:

	overrides:
	  author: U.S. Food and Drug Administration

You can also specify filters to control what datasets from the data.json file are
imported. By default everything is imported. In a "filters" section, map data.json
field names to an array of permitted values or in an "excludes" section map data.json
field names to values or regular expressions surrounded in forward slashes that will
cause datasets to be excluded:

	filters:
		theme: ["Health"]
	excludes:
		accessURL: ["http://example.org/dataset", "/^http://some-pattern/here/"]

Each dataset is compared to each filter and exclude (in this example, a theme filter
and an accessURL exclude). To be imported, the dataset must match at least one value
of a filter and may not match any value of an exclude. In this example, all imported
datasets will have Health set as their theme and no accessURL will be either
"http://example.org/dataset" or start with "http://some-pattern/here" (not that the
final slash indicates the end of the regular expression).

Credit / Copying
----------------

Original work written by the HealthData.gov team. It has been modified in support of Data.gov.

As a work of the United States Government, this package is in the public 
domain within the United States. Additionally, we waive copyright and 
related rights in the work worldwide through the CC0 1.0 Universal 
public domain dedication (which can be found at http://creativecommons.org/publicdomain/zero/1.0/).
