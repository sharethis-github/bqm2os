FROM python:3.6
RUN apt-get install -y g++
RUN pip install --upgrade pip
RUN pip install coverage
RUN mkdir /python
ENV PYTHONPATH /python
RUN apt-get update
RUN apt-get install -y vim
RUN apt-get install -y jq
RUN apt-get install -y unzip
ADD /root /root
RUN pip install gevent
RUN pip install cython
RUN pip install frozendict

## aws client
RUN pip install --upgrade awscli
RUN pip install google-api-python-client coverage boto boto3 pycodestyle requests mock || exit 1
RUN pip install --upgrade google-cloud-core==0.27.1
RUN pip install --upgrade google-cloud-bigquery==0.27.0
RUN pip install --upgrade google-cloud-storage==1.5.0
RUN apt-get install graphviz -y
ADD /python /python
