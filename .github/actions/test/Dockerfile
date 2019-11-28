FROM dajobe/hbase:latest

RUN apt-get update
RUN apt-get install -y 'python3.6' 'python3.7' 'python3.6-dev' 'python3.7-dev' python3-pip net-tools git

COPY . .

RUN chmod +x entrypoint.sh
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
