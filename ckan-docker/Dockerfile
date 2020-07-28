FROM openknowledge/ckan-dev:2.8

RUN apk add py-cryptography libressl-dev musl-dev libffi-dev vim

# Fix shapely lib error. Install geos
# TODO consider moving to Alpine>=3.11 or to Bionic

ADD http://download.osgeo.org/geos/geos-3.7.0.tar.bz2 /geos/geos.tar.bz2
RUN tar xf /geos/geos.tar.bz2 -C /geos --strip-components=1
RUN cd /geos && \
    ./configure && \
    make -j 1 && \
    make install
# Install any extensions needed by your CKAN instance
# (Make sure to add the plugins to CKAN__PLUGINS in the .env file)

RUN pip install -e git+https://github.com/ckan/ckanext-spatial#egg=ckanext-spatial && \
    pip install -r https://raw.githubusercontent.com/ckan/ckanext-spatial/master/pip-requirements.txt && \
    pip install -e git+https://github.com/ckan/ckanext-harvest.git#egg=ckanext-harvest && \
    pip install -r https://raw.githubusercontent.com/ckan/ckanext-harvest/master/pip-requirements.txt && \
    pip install -e git+https://github.com/GSA/ckanext-geodatagov.git#egg=ckanext-geodatagov && \
    pip install -r https://raw.githubusercontent.com/GSA/ckanext-geodatagov/master/pip-requirements.txt && \
    pip install -e git+https://github.com/GSA/ckanext-datagovtheme.git#egg=ckanext-datagovtheme