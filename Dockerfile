FROM python:3.12-slim

RUN apt-get update && apt-get install -y gcc python3-dev gettext-base

RUN mkdir /app

COPY requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

COPY test-requirements.txt /app/test-requirements.txt

RUN pip install -r /app/test-requirements.txt

COPY kinesis /app/kinesis/
COPY tests /app/tests/

WORKDIR /app/
