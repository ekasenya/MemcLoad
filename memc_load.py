#!/usr/bin/env python
# -*- coding: utf-8 -*-
import glob
import os
import gzip
import sys
import logging
import collections
import time
import threading
from queue import Queue
from functools import partial
from multiprocessing import Pool
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

NORMAL_ERR_RATE = 0.01
MAX_RETRY_ATTEMPTS = 5
MEMCACHED_TIMEOUT = 10
CHUNK_SIZE = 50

AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print('{}  {:2.2f} ms'.format(method.__name__, (te - ts) * 1000))
        return result
    return timed


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


# Multi-process strategy

class MemcachedWorker(threading.Thread):
    def __init__(self, memc_addr, task_queue, results, dry):
        threading.Thread.__init__(self)

        self.task_queue = task_queue
        self.memc_client = None
        self.memc_addr = memc_addr
        self.processed = 0
        self.errors = 0

        self.dry = dry
        self.results = results

    def run(self):
        while True:
            values = self.task_queue.get()
            if values == 'quit':
                break

            try:
                if self.dry:
                    for key, packed in values.items():
                        unpacked = appsinstalled_pb2.UserApps()
                        unpacked.ParseFromString(packed)
                        logging.debug("{} - {} -> {}".format(self.memc_addr, key, str(unpacked).replace("\n", " ")))
                        self.processed += 1
                    continue

                if self.insert_memcache(values):
                    self.processed += len(values)
                else:
                    self.errors += len(values)

            except Exception:
                self.errors += len(values)
            self.task_queue.task_done()

        self.results.put((self.processed, self.errors))

    def insert_memcache(self, appsinstalled_dict):
        try:
            if not self.memc_client:
                self.memc_client = memcache.Client([self.memc_addr], socket_timeout=MEMCACHED_TIMEOUT)
            for attempt_num in range(MAX_RETRY_ATTEMPTS):
                try:
                    self.memc_client.set_multi(appsinstalled_dict)
                except Exception:
                    if attempt_num == MAX_RETRY_ATTEMPTS - 1:
                        raise
                    else:
                        time.sleep(attempt_num * 10)

        except Exception as e:
            logging.exception("Cannot write to memc {}: {}".format(self.memc_addr, e))
            return False


@timeit
def multiprocess_strategy(options, device_memc):
    pool = Pool(os.cpu_count())
    for path in pool.map(partial(multithread_process_file, options=options, device_memc=device_memc),
                         [fn for fn in glob.iglob(options.pattern)]):
        dot_rename(path)


def parse_appsinstalled(line):
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


def make_ua(appsinstalled):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    ua.apps.extend(appsinstalled.apps)

    return ua


def multithread_process_file(fn, options, device_memc):
    processed = errors = 0
    workers = []
    results = Queue()
    task_queue_list = {}

    record_buf = {}

    for memc_addr in device_memc.values():
        task_queue = Queue()
        worker = MemcachedWorker(memc_addr, task_queue, results, options.dry)
        workers.append(worker)
        task_queue_list[memc_addr] = task_queue
        record_buf[memc_addr] = {}
        worker.start()

    logging.info('Processing {}'.format(fn))
    fd = gzip.open(fn)
    for line in fd:
        line = line.strip().decode('utf-8')
        if not line:
            continue

        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            errors += 1
            continue
        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            errors += 1
            logging.error("{}. Unknown device type: {}".format(fn, appsinstalled.dev_type))
            continue

        ua = make_ua(appsinstalled)
        key = "{}:{}".format(appsinstalled.dev_type, appsinstalled.dev_id)
        record_buf[memc_addr][key] = ua.SerializeToString()

        if len(record_buf[memc_addr]) >= CHUNK_SIZE:
            task_queue_list[memc_addr].put(record_buf[memc_addr])
            record_buf[memc_addr] = {}

    for memc_addr, task_queue in task_queue_list.items():
        if len(record_buf[memc_addr]) > 0:
            task_queue_list[memc_addr].put(record_buf[memc_addr])
        task_queue.put('quit')

    for worker in workers:
        worker.join()

    while not results.empty():
        res = results.get()
        processed += res[0]
        errors += res[1]

    if not processed:
        fd.close()
        return fn

    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        logging.info("{}. Acceptable error rate ({}). Successfull load".format(fn, err_rate))
    else:
        logging.error("{}. High error rate ({} > {}). Failed load".format(fn, err_rate, NORMAL_ERR_RATE))
    fd.close()

    return fn


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    multiprocess_strategy(options, device_memc)


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
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
