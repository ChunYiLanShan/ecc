FROM python:2
COPY . /opt/app
WORKDIR /opt/app
ENV LD_LIBRARY_PATH '/opt/app/instantclient_18_3'
RUN apt update
RUN apt install libaio1
RUN apt install -y mysql-client
# sudo apt-get install python-pip python-dev build-essential ?
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
CMD python -m SimpleHTTPServer
