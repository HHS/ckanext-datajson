#!/bin/bash
set -e

echo "Updating script ..."

wget https://raw.githubusercontent.com/GSA/catalog.data.gov/master/tools/ci-scripts/circleci-build-catalog-next.bash
wget https://raw.githubusercontent.com/GSA/catalog.data.gov/master/ckan/test-catalog-next.ini

sudo chmod +x circleci-build-catalog-next.bash
source circleci-build-catalog-next.bash

echo "Update ckanext-datajson"
python setup.py develop

echo "TESTING ckanext-datajson"
nosetests --ckan --with-pylons=test-catalog-next.ini ckanext/datajson/tests/nose --debug=ckanext
