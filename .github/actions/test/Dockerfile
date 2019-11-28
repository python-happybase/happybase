FROM dajobe/hbase:latest

ENV PYTHON_VESRION=3.7
ENV PYTHON=python$PYTHON_VERSION

RUN apt-get update
RUN apt-get install -y $PYTHON $PYTHON-dev python3-pip net-tools

COPY . .

RUN chmod +x entrypoint.sh
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
