#!/usr/bin/env bash

set -xe

/opt/hbase-server & > /dev/null  # It's already got its own logs
sleep 3  # Give it time to start up
/usr/bin/${PYTHON} -m unittest  # TEST
