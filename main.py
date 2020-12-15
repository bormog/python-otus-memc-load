# -*- coding: utf-8 -*-

import collections
import glob
import gzip
import logging
import multiprocessing as mp
import os
import sys
import threading
import time
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from optparse import OptionParser
import appsinstalled_pb2
from memc import MemcacheClient

success = failed = 0

"""
directory: [filepath, filepath, filepath, ...]

 ||
 || lines_producer: threading
 \/

queue: [ task, task, task, ...], task = [line, line, line, ... ]

 ||
 || lines_worker: multiprocessing
 \/

4 x queue: [ task, task, task, ...], task = [ [key, packed], [key, packed], ...]

 ||
 || memcache_consumer: threading
 \/

memcache

"""


NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple('AppsInstalled', ['dev_type', 'dev_id', 'lat', 'lon', 'apps'])


def dot_rename(path):
    """
    Rename file after work is done
    """
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def serialize_appsinstalled(appsinstalled):
    """
    Serialize AppsInstalled object into protobuff
    """
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    return key, packed


def parse_appsinstalled(line):
    """
    Convert line from file into AppInstalled Object
    """
    line = line.decode('utf-8')
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def lines_producer(filepath, dst_queue, chunk_size, dry_run=False):
    """
    Read lines from filepath, create chunks by chunk_size and send in dst_queue
    """
    if not os.path.isfile(filepath):
        logging.error('File not found by path %s' % filepath)
        return False

    with gzip.open(filepath) as fr:
        logging.info('Start processing %s' % filepath)
        chunk = []
        for line in fr:
            line = line.strip()
            if not line:
                continue
            chunk.append(line)

            if len(chunk) == chunk_size:
                dst_queue.put(chunk)
                chunk = []
        if len(chunk):
            dst_queue.put(chunk)
    logging.info('Finish processing %s' % filepath)
    if not dry_run:
        logging.info('Rename file after processing %s' % filepath)
    return True


class BufferQueueRouter:
    """
    Create chunk by max_size and send data in queue
    """

    def __init__(self, queues, max_size):
        self._queues = queues
        self._buffer = {key: [] for key in queues.keys()}
        self._max_size = max_size

    def set(self, key, task):
        self._buffer[key].append(task)
        if self._max_size == len(self._buffer[key]):
            self._put_buffer(key)

    def _put_buffer(self, key):
        logging.debug('Put %s tasks in queue %s' % (len(self._buffer[key]), key))
        self._queues.get(key).put(self._buffer[key])
        self._buffer[key] = []

    def flush(self):
        for key in self._buffer.keys():
            if len(self._buffer[key]):
                self._put_buffer(key)


def lines_worker(src_queue, dst_queues, memcache_addresses, chunk_size, return_dict):
    """
    Read lines from queue, pack them into protobuff object and send data in dst_queue
    """
    errors = 0
    router = BufferQueueRouter(queues=dst_queues, max_size=chunk_size)
    while True:
        lines = src_queue.get()
        if isinstance(lines, str) and lines == 'quit':
            router.flush()
            break

        for line in lines:
            apps_installed = parse_appsinstalled(line)
            if not apps_installed:
                errors += 1
                continue
            memc_address = memcache_addresses.get(apps_installed.dev_type)
            if not memc_address:
                errors += 1
                logging.error("Unknown device type: %s" % apps_installed.dev_type)
                continue

            key, packed = serialize_appsinstalled(apps_installed)
            router.set(memc_address, (key, packed,))

    return_dict[os.getpid()] = errors
    logging.debug('Finish processed lines in memcache queue, with errors = %s' % errors)


def memcache_consumer(src_queue, addr, memcache_client, lock, dry_run=False):
    global success
    global failed
    """
    Read data from queue and save in memcache
    """
    while True:
        lines = src_queue.get()
        if isinstance(lines, str) and lines == 'quit':
            break

        mapping = {key: packed for key, packed in lines}
        if dry_run:
            logging.debug("%s -> %s" % (addr, len(mapping.keys())))
        else:
            try:
                not_set_keys = memcache_client.set_multi(addr, mapping)

                _failed = len(not_set_keys)
                _success = len(mapping.keys()) - _failed

                with lock:
                    success += _success
                    failed += _failed

                logging.debug('Inserting data in memc %s, errors = %s' % (addr, len(not_set_keys)))
            except Exception as e:
                logging.exception("Cannot write to memc %s: %s" % (addr, e))


class MemcacheQueueManager:
    """
    Wrapper for manipulate different queues
    """

    def __init__(self, addresses, maxsize=0):
        self._queues = {address: mp.Queue(maxsize=maxsize) for address in addresses}

    def get(self, key):
        return self._queues.get(key)

    def keys(self):
        return self._queues.keys()

    def __iter__(self):
        for key, queue in self._queues.items():
            yield key, queue


class ConsumerPool:
    """
    Wrapper for creating a list of queue consumers
    """

    def __init__(self, que, cls, size, target, args):
        self._que = que
        self._consumers = []

        for _ in range(size):
            _args = (self._que,) + args
            self._consumers.append(cls(target=target, args=_args))

    def start(self):
        for consumer in self._consumers:
            consumer.start()

    def stop(self):
        for _ in self._consumers:
            self._que.put('quit')

    def join(self):
        for consumer in self._consumers:
            consumer.join()


def main(options):
    ts = time.time()

    device_memc = {
        'idfa': options.idfa,
        'gaid': options.gaid,
        'adid': options.adid,
        'dvid': options.dvid,
    }

    memcache_client = MemcacheClient(device_memc.values())

    files = glob.iglob(options.pattern)

    lock = threading.Lock()
    lines_queue = mp.Queue()
    memcache_queue_manager = MemcacheQueueManager(device_memc.values())
    return_dict = mp.Manager().dict()

    # Convert lines in protobuff objects and put in queue
    lines_worker_pool = ConsumerPool(que=lines_queue,
                                     size=options.workers_count,
                                     cls=mp.Process,
                                     target=lines_worker,
                                     args=(memcache_queue_manager,
                                           device_memc,
                                           options.worker_buff_size,
                                           return_dict
                                           )
                                     )
    lines_worker_pool.start()

    # Put lines from files in queue
    lines_producer_pool = ThreadPool(processes=options.producers_count)
    job = partial(lines_producer, dst_queue=lines_queue, chunk_size=options.producer_buff_size, dry_run=options.dry)
    lines_producer_pool.map(job, files)

    # Save data in memcache
    memcache_consumer_pool = {}
    for addr, que in memcache_queue_manager:
        memcache_consumer_pool[addr] = ConsumerPool(
            que=que,
            size=options.consumers_count,
            cls=threading.Thread,
            target=memcache_consumer,
            args=(addr, memcache_client, lock, options.dry,)
        )
    for pool in memcache_consumer_pool.values():
        pool.start()

    lines_producer_pool.close()
    lines_producer_pool.join()

    lines_worker_pool.stop()
    lines_worker_pool.join()

    for pool in memcache_consumer_pool.values():
        pool.stop()
        pool.join()

    errors = float(sum(return_dict.values()) + failed)
    err_rate = errors / (errors + success)
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
    logging.info('Execution took {:.4f}'.format(time.time() - ts))


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    # @TODO rename file after work is done
    # @TODO avoid logging lock in process spawn
    # @TODO write some tests

    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    # op.add_option("--pattern", action="store", default="data/appsinstalled/*.tsv.gz")
    op.add_option("--pattern", action="store", default="test_data/*.tsv.gz")

    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")

    op.add_option('--producers_count', action='store', default=4)
    op.add_option('--workers_count', action='store', default=mp.cpu_count())
    op.add_option('--consumers_count', action='store', default=2)
    op.add_option('--producer_buff_size', action='store', default=3000)
    op.add_option('--worker_buff_size', action='store', default=2000)

    (opts, args) = op.parse_args()

    logging.basicConfig(filename=opts.log,
                        level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S'
                        )
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
