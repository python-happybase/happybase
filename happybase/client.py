# -*- coding: utf-8 -*-
from thriftpy.thrift import TClient
from thriftpy.thrift import TApplicationException
from thriftpy.transport import TTransportException
from socket import error as socket_error
from collections import deque
from time import sleep
import logging


logger = logging.getLogger(__name__)


class RecoveringClient(TClient):
    def __init__(self, *args, **kwargs):
        self._connection = kwargs.pop("connection", None)
        self._retries = kwargs.pop("retries", (0, 5, 30))
        super(RecoveringClient, self).__init__(*args, **kwargs)

    def _req(self, _api, *args, **kwargs):
        no_retry = kwargs.pop("no_retry", False)
        retries = deque(self._retries)
        interval = 0
        client = super(RecoveringClient, self)
        while True:
            try:
                return client._req(_api, *args, **kwargs)
            except (TApplicationException, socket_error, TTransportException) as exc:
                logger.exception("Got exception")
                while True:
                    interval = retries.popleft() if retries else interval
                    logger.info("Sleeping for %d seconds", interval)
                    sleep(interval)
                    logger.info("Trying to reconnect")
                    try:
                        self._connection._refresh_thrift_client()
                        self._connection.open()
                        client = super(RecoveringClient, self._connection.client)
                        logger.debug("New client is initialized")
                    except TTransportException:
                        logger.exception("Got exception, while trying to reconnect. Continuing")
                        pass
                    else:
                        break
                if no_retry:
                    raise exc