import memcache
import time
import logging

RETRY_MAX_ATTEMPTS = 3
RETRY_DELAY = 0.1
MEMCACHE_SOCKET_TIMEOUT = 30


def retry(raise_on_failure=True, retry_max_attempts=None, retry_delay=None):
    def retry_on_failure(method):

        def wrapper(*args, **kwargs):
            nonlocal retry_max_attempts, retry_delay

            if retry_max_attempts is None:
                retry_max_attempts = RETRY_MAX_ATTEMPTS
            if retry_delay is None:
                retry_delay = RETRY_DELAY

            last_exception = None
            for i in range(retry_max_attempts):
                try:
                    return method(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    time.sleep(retry_delay)
                    retry_delay *= 2

            if last_exception is not None:
                msg = 'Method %s was failed after %d attempts' % (method, RETRY_MAX_ATTEMPTS)
                logging.exception(msg, exc_info=last_exception)

            if raise_on_failure:
                raise last_exception

        return wrapper

    return retry_on_failure


class MemcacheClient:
    """
    Wrapper for memcache
    """

    def __init__(self, addresses):
        self._clients = {address: memcache.Client([address], socket_timeout=MEMCACHE_SOCKET_TIMEOUT)
                         for address in addresses}

    @retry(raise_on_failure=True)
    def set_multi(self, address, mapping):
        """ Sets multiple keys in the memcache doing just one query.

        @param address: Memcache address
        @param mapping: A dict of key/value pairs to set.
        @return: List of keys which failed to be stored [ memcache out
           of memory, etc. ].

        @rtype: list
        """
        client = self._clients.get(address)
        return client.set_multi(mapping)
