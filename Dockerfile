FROM python:3.9.7-slim as intermediate

RUN apt-get update

RUN apt-get install -y git

ARG SSH_PRIVATE_KEY

RUN mkdir /root/.ssh/

RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa

RUN chmod 600 /root/.ssh/id_rsa

RUN touch /root/.ssh/known_hosts

RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

ADD ./app/requirements.txt /

WORKDIR /pip-packages/

RUN pip download -r /requirements.txt

FROM python:3.9.7-slim

ENV PYTHONPATH=/home/pipeline:$PYTHONPATH

WORKDIR /pip-packages/

COPY --from=intermediate /pip-packages/ /pip-packages/

RUN pip install --no-index --find-links=/pip-packages/ /pip-packages/*

RUN mkdir -p /home/pipeline

ADD ./app /home/pipeline/

WORKDIR /home/pipeline