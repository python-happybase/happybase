#!/usr/bin/env bash

set -xe

${PYTHON} -m pip install -U -r test-requirements.txt

/opt/hbase-server & &> /dev/null  # It's already got its own logs
# Wait for thrift port to bind
while ! netstat -tna | grep 'LISTEN\>' | grep -q ':9090\>'; do sleep 1; done
sleep 1  # Give it a little extra time
${PYTHON} -m unittest  # TEST
