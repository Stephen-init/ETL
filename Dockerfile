FROM python:3.9.7-slim

ENV PYTHONPATH=/home/pipeline:$PYTHONPATH

RUN mkdir -p /home/pipeline

COPY ./app /home/pipeline/

RUN pip install -r /home/pipeline/requirements.txt

WORKDIR /home/pipeline