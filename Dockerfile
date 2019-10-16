FROM python:3.7-alpine

COPY . /test_storages
WORKDIR /test_storages
RUN apk update && apk upgrade && apk add make
RUN pip install -r requirements/dev.txt -r requirements/prod.txt
CMD tail -f -s 60 /dev/null
