## Using the Docker Dev Environment

### Build Environment

To start environment, run:
```docker-compose build```
```docker-compose up```

CKAN will start at localhost:5000

To shut down environment, run:

```docker-compose down```

To docker exec into the CKAN image, run:

```docker-compose exec ckanextgeodatagov /bin/bash```

### Run Tests with Docker

```docker-compose exec ckanextgeodatagov /bin/bash -c "nosetests --ckan --with-pylons=src_extensions/geodatagov/test.ini src_extensions/geodatagov/"```
