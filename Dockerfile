FROM python:3.7-alpine

COPY . /test_storages
WORKDIR /test_storages
RUN pip install -r requirements/dev.txt -r requirements/prod.txt
CMD tail -f -s 60 /dev/null
