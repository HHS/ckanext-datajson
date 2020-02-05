## Using the Docker Dev Environment

### Build Environment

To start environment, run:
```docker-compose build```
```docker-compose up```

To shut down environment, run"

```docker-compose down```

CKAN will start at localhost:5000


### Run Tests with Docker

Make sure docker environment is running.

Docker exec into the CKAN container:

```docker exec -it ckanext-datajson_ckanextdatajson_1 /bin/bash -c "export TERM=xterm; exec bash"```

Run the tests:

```nosetests --ckan --with-pylons=src_extensions/datajson/test.ini src_extensions/datajson/```