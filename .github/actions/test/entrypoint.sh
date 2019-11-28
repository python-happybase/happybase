#!/usr/bin/env bash

set -xe

PYTHON=/usr/bin/python${PYTHON_VERSION}

${PYTHON} -m pip install -U -r test-requirements.txt

/opt/hbase-server & &> /dev/null  # It's already got its own logs
# Wait for thrift port to bind
while ! netstat -tna | grep 'LISTEN\>' | grep -q ':9090\>'; do sleep 1; done
sleep 1  # Give it a little extra time
${PYTHON} -m unittest  # TEST

if [ "${PYTHON_VERSION}" == "3.7" ]; then
  ${PYTHON} -m pip install coverage codecov
  ${PYTHON} -m coverage run -m unittest
  ${PYTHON} -m coverage xml
  ${PYTHON} -m codecov --token=${CODECOV_TOKEN}
fi