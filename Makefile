build: ## Build the docker containers
	docker-compose build

clean: ## Clean workspace and containers
	find . -name *.pyc -delete
	docker-compose down -v --remove-orphan

test: ## Run tests in an existing container
	@# TODO wait for CKAN to be up; use docker-compose run instead
	docker-compose exec ckan /bin/bash -c "nosetests --ckan --with-pylons=src/ckan/test-catalog-next.ini src_extensions/datajson/ckanext/datajson/tests"

up: ## Start the containers
	docker-compose up


.DEFAULT_GOAL := help
.PHONY: build clean help test up

# Output documentation for top-level targets
# Thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
help: ## This help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
