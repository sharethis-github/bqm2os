FROM python
RUN apt-get install g++
RUN pip install --upgrade pip
RUN pip install coverage
RUN mkdir /python
ENV PYTHONPATH /python
RUN apt-get update
RUN apt-get install vim -y
RUN apt-get install jq
RUN apt-get install unzip
ADD /root /root
RUN pip install gevent
RUN pip install cython

## aws client
RUN pip install --upgrade awscli
RUN pip install google-api-python-client coverage boto boto3 pep8 requests mock || exit 1
RUN pip install --upgrade google-cloud-bigquery
ADD /python /python
