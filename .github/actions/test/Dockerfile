FROM dajobe/hbase:latest

RUN apt-get update
RUN apt-get install -y 'python3.6' 'python3.7' 'python3.6-dev' 'python3.7-dev' python3-pip

COPY . .

RUN 'python3.6' -m pip install -r requirements.txt
RUN 'python3.7' -m pip install -r requirements.txt

RUN chmod +x entrypoint.sh
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
