FROM dajobe/hbase:latest

RUN apt-get update
RUN apt-get install -y 'python3.6' 'python3.7' 'python3.6-dev' 'python3.7-dev' python3-pip

COPY . .

RUN 'python3.6' -m pip install -r test-requirements.txt
RUN 'python3.7' -m pip install -r test-requirements.txt

RUN chmod +x entrypoint.sh
RUN mv entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]
