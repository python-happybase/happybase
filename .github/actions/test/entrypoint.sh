#!/usr/bin/env bash

set -xe

/opt/hbase-server & > /dev/null  # It's already got its own logs
sleep 3  # Give it time to start up
/usr/bin/python${PYTHON_VERSION} -m unittest  # TEST
