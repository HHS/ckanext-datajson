#!/bin/bash
set -e

echo "This is travis-build.bash..."

echo "-----------------------------------------------------------------"
echo "Installing the packages that CKAN requires..."
sudo apt-get update -qq
sudo apt-get install solr-jetty libcommons-fileupload-java \
	libpq-dev postgresql postgresql-contrib redis-server swig

echo "-----------------------------------------------------------------"
echo "Installing CKAN and its dependencies..."

cd .. # CircleCI starts inside ckanext-datajson folder
pwd
ls -la


if [ $CKANVERSION == 'inventory-next' ]
then
	git clone https://github.com/ckan/ckan
	cd ckan	
	git checkout 2.8
elif [ $CKANVERSION == 'inventory' ]
then
	git clone https://github.com/GSA/ckan
	cd ckan	
	git checkout inventory	
fi

pip install pip==20.3.3
pip install testrepository

echo "-----------------------------------------------------------------"
echo "Installing Python dependencies..."

pip install wheel

# https://github.com/GSA/ckanext-datajson/issues/61
pip install setuptools==44.1.1

python setup.py develop
cp ./ckan/public/base/css/main.css ./ckan/public/base/css/main.debug.css
pip install -r requirements.txt
pip install -r dev-requirements.txt

cd ..
echo "-----------------------------------------------------------------"
echo "Setting up Solr..."
# solr is multicore for tests on ckan master now, but it's easier to run tests
# on Travis single-core still.
# see https://github.com/ckan/ckan/issues/2972
sed -i -e 's/solr_url.*/solr_url = http:\/\/127.0.0.1:8983\/solr/' ckan/test-core.ini
printf "NO_START=0\nJETTY_HOST=127.0.0.1\nJETTY_PORT=8983\nJAVA_HOME=$JAVA_HOME" | sudo tee /etc/default/jetty
sudo cp ckan/ckan/config/solr/schema.xml /etc/solr/conf/schema.xml
sudo service jetty restart

echo "-----------------------------------------------------------------"
echo "Creating the PostgreSQL user and database..."
sudo -u postgres psql -c "CREATE USER ckan_default WITH PASSWORD 'pass';"
sudo -u postgres psql -c 'CREATE DATABASE ckan_test WITH OWNER ckan_default;'
sudo -u postgres psql -c 'CREATE DATABASE datastore_test WITH OWNER ckan_default;'

echo "-----------------------------------------------------------------"
echo "Initialising the database..."
cd ckan
paster db init -c test-core.ini

cd ..
echo "-----------------------------------------------------------------"
echo "Installing Harvester"

if [ $CKANVERSION == 'inventory-next' ]
then
	git clone https://github.com/ckan/ckanext-harvest
	cd ckanext-harvest
	git checkout master
elif [ $CKANVERSION == 'inventory' ]
then
	git clone https://github.com/GSA/ckanext-harvest
	cd ckanext-harvest
	git checkout datagov	
fi

python setup.py develop
pip install -r pip-requirements.txt


cd ..

if [ $CKANVERSION == 'inventory-next' ]
then
	pip install -e git+https://github.com/GSA/USMetadata.git@ckan-2-8#egg=ckanext-usmetadata
elif [ $CKANVERSION == 'inventory' ]
then
	pip install -e git+https://github.com/GSA/USMetadata.git@master#egg=ckanext-usmetadata	
fi

pip install ckanext-dcat-usmetadata~=0.2

echo "-----------------------------------------------------------------"
echo "Installing ckanext-datajson and its requirements..."
cd ckanext-datajson
pip install -r pip-requirements.txt
python setup.py develop

echo "-----------------------------------------------------------------"
echo "Moving test.ini into a subdir..."
mkdir subdir
mv test-inventory.ini subdir

echo "travis-build.bash is done."