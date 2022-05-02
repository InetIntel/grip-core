#  This software is Copyright (c) 2015 The Regents of the University of
#  California. All Rights Reserved. Permission to copy, modify, and distribute this
#  software and its documentation for academic research and education purposes,
#  without fee, and without a written agreement is hereby granted, provided that
#  the above copyright notice, this paragraph and the following three paragraphs
#  appear in all copies. Permission to make use of this software for other than
#  academic research and education purposes may be obtained by contacting:
#
#  Office of Innovation and Commercialization
#  9500 Gilman Drive, Mail Code 0910
#  University of California
#  La Jolla, CA 92093-0910
#  (858) 534-5815
#  invent@ucsd.edu
#
#  This software program and documentation are copyrighted by The Regents of the
#  University of California. The software program and documentation are supplied
#  "as is", without any accompanying services from The Regents. The Regents does
#  not warrant that the operation of the program will be uninterrupted or
#  error-free. The end-user understands that the program was developed for research
#  purposes and is advised not to rely exclusively on the program for any reason.
#
#  IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
#  DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
#  PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
#  THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH
#  DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
#  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
#  IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
#  MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

import argparse
import datetime
import logging
import sys
import wandio

from grip.redis.redis_helper import RedisHelper

ADJ_KEY_TMPL = "ADJ:IPV4:%s"
TIMESTAMPS_KEY = "ADJ:TS"

OBJ_TMPL = "year=%04d/month=%02d/day=%02d/triplets-weekly.%d.gz"
DEFAULT_DATADIR="/data/bgp/bgp-hijacks-triplets-weekly"

TIME_GRANULARITY = 7 * 86400


class Adjacencies:

    def __init__(self, window_weeks=52, host=None, port=6379, db=2, log_level="INFO", datadir=None):
        self.window_weeks = int(window_weeks)
        if datadir is not None:
            self.datadir = datadir
        else:
            self.datadir = DEFAULT_DATADIR

        if host is not None:
            self.rh = RedisHelper(host, port, db, log_level)
        else:
            self.rh = RedisHelper(port=port, db=db, log_level=log_level)

    def get_inserted_weeks(self):
        return set([int(ts[1]) for ts in
                    self.rh.zrange(TIMESTAMPS_KEY, 0, -1, withscores=True)])

    def get_current_window(self):
        week_tses = self.get_inserted_weeks()
        if not len(week_tses):
            return None
        return min(week_tses), max(week_tses)

    def find_missing(self, latest_ts, first_ts):
        if latest_ts:
            latest_ts = int(latest_ts)
        if first_ts:
            first_ts = int(first_ts)

        window = self.get_current_window()

        if window is None:
            # DB is empty, so generate all dates for the last window_weeks
            if not first_ts:
                now = latest_ts - (self.window_weeks * 7 * 86400)
            else:
                now = first_ts
        else:
            now = window[1] + TIME_GRANULARITY
        missing = []

        while now <= latest_ts:
            missing.append(now)
            now += TIME_GRANULARITY
        return missing

    def print_window_info(self):
        window = self.get_current_window()
        if window is None:
            window = (None, None)
        print("Current window: [%s, %s]" % window)

    def insert_adj_file(self, path, ts=None):
        logging.info("Inserting adjacencies file: %s" % path)

        if ts is None:
            # TODO: this is fragile. fix it
            ts = int(path.split(".")[1])

        # checking we are not inserting an older file
        window = self.get_current_window()
        if window is not None and ts <= window[1]:
            logging.error("Cannot insert data before %d (Tried to insert %d)"
                          % (window[1], ts))
            return

        pipe = self.rh.get_pipeline()

        # add this (week) timestamp to the list of inserted timestamps
        pipe.zadd(TIMESTAMPS_KEY, ts, ts)

        # to de-duplicate the pairs
        adj_temp = set()
        try:
            with wandio.open(path) as fh:
                for line in fh:
                    # skip AS sets
                    if "{" in line:
                        continue
                    _, asn_list = line.strip().split("|")
                    triplet = asn_list.split(" ")
                    assert len(triplet) == 2 or len(triplet) == 3
                    last = None
                    for this in triplet:
                        if last is None:
                            last = this
                            continue
                        if (last, this) not in adj_temp:
                            pipe.zadd(ADJ_KEY_TMPL % last, ts, "%s:%x" % (this, ts))
                            adj_temp.add((last, this))
                        last = this
        except IOError as e:
            logging.error("Could not read pfx-origin file '%s'" % path)
            logging.error("I/O error: %s" % e.strerror)
            return

        res = pipe.execute()
        logging.info("Inserted %d adjacencies" % (sum(int(i) for i in res) - 1))

    def insert_adj_timestamp(self, unix_ts):
        ts = datetime.datetime.utcfromtimestamp(unix_ts)
        obj = OBJ_TMPL % (ts.year, ts.month, ts.day, unix_ts)
        file_path = "%s/%s" % (self.datadir, obj)
        self.insert_adj_file(file_path)

    def clean(self, latest_ts):
        pipe = self.rh.get_pipeline()

        window = self.get_current_window()
        new_oldest_ts = latest_ts - (self.window_weeks * 7 * 86400)
        logging.info("Removing data < than %d" % new_oldest_ts)

        if window is None:
            logging.info("DB is empty. Nothing to clean.")
            return

        for key in self.rh.scan_keys(ADJ_KEY_TMPL % "*"):
            # remove anything < (but not =) to new_oldest_ts
            pipe.zremrangebyscore(key, "-inf", "(%s" % new_oldest_ts)
        res = pipe.execute()
        logging.info("Removed %d adjacencies" % sum(int(i) for i in res))

        # and update the list of inserted timestamps
        self.rh.zremrangebyscore(TIMESTAMPS_KEY, "-inf", "(%s" % new_oldest_ts)

    def is_neighbor(self, asn, neighbor_asn, min_ts=None, max_ts=None):
        """
        return true if asn_neighbor is a neighbor of asn
        """
        neighbors = self.get_neighbors(asn, min_ts=min_ts, max_ts=max_ts)
        return neighbor_asn in neighbors

    def is_neighbor_historical(self, asn, neighbor_asn):
        """
        return true if asn_neighbor is a neighbor of asn, excluding the most
        recent week of data
        """
        end_ts = self.get_current_window()[1] - 86400 * 7
        return self.is_neighbor(asn, neighbor_asn, max_ts=end_ts)

    @staticmethod
    def _extract_asn_from_res(redis_result):
        # ("ASN:HEX_TIME", TIME)
        return redis_result[0].split(":")[0]

    def get_neighbors(self, asn, min_ts=None, max_ts=None, with_timestamps=False):
        """
        return the list of neighbors for a certain interval of time
        """
        if min_ts is None:
            min_ts = "-inf"
        if max_ts is None:
            max_ts = "+inf"
        neighbors = self.rh.zrangebyscore(ADJ_KEY_TMPL % asn,
                                          min_ts, max_ts, withscores=True)
        if with_timestamps:
            return set([(self._extract_asn_from_res(n), n[1]) for n in neighbors])
        return set([self._extract_asn_from_res(n) for n in neighbors])


def main():
    parser = argparse.ArgumentParser(description="""
    Utilities for populating and managing the "adjacencies" redis database.
    """)
    parser.add_argument('-f', "--file", action="store", default=None,
                        help="triplets-weekly file")

    parser.add_argument('-t', "--timestamp", action="store", default=None,
                        help="Insert data for given timestamp")

    parser.add_argument('-r', "--redis-host", action="store",
                        default=None, help='Redis address')

    parser.add_argument('-p', "--redis-port", action="store", default=6379,
                        help='Redis port')

    parser.add_argument('-D', "--data-directory", action="store",
                        help='Directory to store triplet files',
                        default=DEFAULT_DATADIR)

    parser.add_argument('-d', "--redis-db", action="store",
                        help='Redis database', default=2)

    parser.add_argument('-n', "--neighbors", action="store",
                        help='Get a list of neighbors '
                             '(or check if A_B are neighbors)')

    parser.add_argument('-s', "--show-window", action="store_true",
                        default=False, help="Show the current window")

    parser.add_argument('-m', "--missing", action="store_true",
                        default=False, help="List (week) timestamps missing")

    parser.add_argument('-w', "--window-weeks", action="store",
                        default=52, help="Length of the window (in weeks)")

    parser.add_argument('-l', "--latest-ts", action="store",
                        default=None, help="Timestamp of the most recent data (required for --clean and --missing)")

    parser.add_argument('-L', "--first-ts", action="store",
                        default=None, help="Timestamp of the earliest data (required for --clean and --missing)")

    parser.add_argument('-c', "--clean", action="store_true", default=False,
                        help="Clean data outside window")

    parser.add_argument('-v', "--verbose", action="store_true", default=False,
                        help="Print debugging information")

    opts = parser.parse_args()

    adj = Adjacencies(
        int(opts.window_weeks),
        opts.redis_host,
        opts.redis_port,
        opts.redis_db,
        "DEBUG" if opts.verbose else "INFO",
        opts.data_directory
    )

    if opts.neighbors:
        if "_" in opts.neighbors:
            print(adj.is_neighbor(*opts.neighbors.split("_")))
        else:
            for n in adj.get_neighbors(opts.neighbors):
                print(n)
        return

    if (opts.missing or opts.clean) and opts.latest_ts is None:
        logging.error("--latest-ts must be specified")
        sys.exit(-1)

    if opts.show_window:
        adj.print_window_info()

    if opts.missing:
        missing = adj.find_missing(latest_ts=opts.latest_ts, first_ts=opts.first_ts)
        for miss in missing:
            print(miss)

    if opts.timestamp is not None:
        adj.insert_adj_timestamp(int(opts.timestamp))

    if opts.file is not None:
        adj.insert_adj_file(opts.file)

    if opts.clean:
        adj.clean(int(opts.latest_ts))
