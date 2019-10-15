.PHONY: build
.ONESHELL:
build:
	@ docker build . -t test_storages

.PHONY: run
.ONESHELL:
run:
	@ docker run -d -v "$$(pwd):/test_storages" --rm --name test_storages test_storages

.PHONY: ssh
.ONESHELL:
ssh:
	@ docker exec -it test_storages /bin/sh

.PHONY: stop
.ONESHELL:
stop:
	docker container stop  $$(docker container ls -q --filter name=test_storages)
