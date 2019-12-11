#!/bin/sh -e

echo "TESTING ckanext-datajson"

nosetests --ckan --with-pylons=subdir/test.ini ckanext/datajson 