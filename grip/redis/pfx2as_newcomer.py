#!/usr/bin/env python

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
import swiftclient
import wandio
import logging

from grip.redis.redis_helper import RedisHelper

REDIS_ROOT_PFX = "PFX:DAY"
REDIS_P2A_PFX = "%s:IPV4" % REDIS_ROOT_PFX  # pfx2as redis prefix
REDIS_A2P_PFX = "%s:AS" % REDIS_ROOT_PFX  # as2pfx redis prefix

TIMESTAMPS_KEY = "%s:TIMESTAMPS" % REDIS_ROOT_PFX
TIME_GRANULARITY = 300
DEFAULT_WINDOW_HOURS = 24
SWIFT_CONTAINER = "bgp-hijacks-pfx-origins"
SWIFT_OBJ_TMPL = "year=%04d/month=%02d/day=%02d/hour=%02d/pfx-origins.%d.gz"


class Pfx2AsNewcomer:

    def __init__(self, window_hours=DEFAULT_WINDOW_HOURS, host=None, port=6379, db=1, log_level="INFO"):
        self.window_hours = window_hours
        if host is not None:
            self.rh = RedisHelper(host, port, db, log_level)
        else:
            self.rh = RedisHelper(port=port, db=db, log_level=log_level)

    def get_timestamps(self, as_set=False):
        # MW: it checks the timestamp for pfx2as data only (not as2pfx)
        timestamps = [int(ts[1]) for ts in self.rh.zrange(TIMESTAMPS_KEY, 0, -1, withscores=True)]

        if as_set:
            return set(timestamps)

        return timestamps

    def get_most_recent_timestamp(self, max_ts):
        # try to get the maximum time-stamp in redis here.
        timestamps = self.get_timestamps()
        # specified maximum timestamps, get the closest one
        recent_ts = None
        for t in reversed(timestamps):
            # walk through timestamps, in reverse order, i.e. from highest to lowest
            if t <= max_ts:
                recent_ts = t
                break
        return recent_ts

    # returned window is EXCLUSIVE, INCLUSIVE
    def get_current_window(self, timestamps=None):
        # ask redis for the timestamps key
        if timestamps is None:
            timestamps = self.get_timestamps()
        if timestamps is None or not len(timestamps):
            return None
        max_ts = timestamps[-1]
        min_ts = max_ts - (self.window_hours * 3600)
        return min_ts, max_ts

    def get_outside_window(self, window=None):
        timestamps = self.get_timestamps()
        if window is None:
            window = self.get_current_window(timestamps)
        if window is None:
            # if the window is empty, everything is outside
            return timestamps
        outside = []
        for ts in timestamps:
            if ts <= window[0] or ts > window[1]:
                outside.append(ts)
        return outside

    def find_missing_inside_window(self, window=None):
        if window is None:
            window = self.get_current_window()
        if window is None:
            # window is empty, there can't be anything missing
            return []
        tsset = self.get_timestamps(as_set=True)
        missing = []
        now = window[0] + TIME_GRANULARITY
        while now <= window[1]:
            if now not in tsset:
                missing.append(now)
            now += TIME_GRANULARITY
        return missing

    def print_window_info(self):
        window = self.get_current_window()
        if window is None:
            window = (None, None)
        print("Current window: (%s, %s]" % window)
        outside = self.get_outside_window()
        if len(outside):
            print("%d timestamps outside window:" % len(outside))
            for ts in outside:
                print(" - %s" % ts)
        else:
            print("No timestamps outside window")
        missing = self.find_missing_inside_window()
        if len(missing):
            print("%d timestamps missing inside window:" % len(missing))
            for ts in missing:
                print(" - %s" % ts)
        else:
            print("No timestamps missing inside window")

    def remove_outside_window(self):
        window = self.get_current_window()
        if not len(self.get_outside_window(window)):
            logging.info("Nothing to remove outside window")
            return
        logging.info("Removing data <= %s" % window[0])
        pipe = self.rh.get_pipeline()
        for key in self.rh.scan_keys(REDIS_ROOT_PFX + "*"):  # MW: this should match both pfx2as and as2pfx
            pipe.zremrangebyscore(key, "-inf", window[0])
        res = pipe.execute()
        logging.info("Removal finished (%s)" % len(res))

    def insert_pfx_file(self, path, force=False):
        pipe = self.rh.get_pipeline()
        logging.info("Inserting pfx2as mappings from %s" % path)

        cur_ts = self.get_timestamps(as_set=True)
        window = self.get_current_window()

        as2pfx_dict = {}
        file_timestamp = 0
        try:
            with wandio.open(path) as fh:
                for line in fh:
                    # 1476104400|115.116.0.0/16|4755|4755|STABLE
                    timestamp, prefix, old_asn, new_asn, label = line.strip().split("|")
                    timestamp = int(timestamp)
                    if file_timestamp == 0:
                        file_timestamp = timestamp
                    elif timestamp != file_timestamp:
                        raise ValueError("Multiple timestamps in one file", path)

                    if label == "REMOVED" or ":" in prefix:
                        # do not insert prefixes that are no longer announced
                        # we also do not (currently) support IPv6 prefixes
                        continue
                    if not force and window is not None and (timestamp in cur_ts or timestamp < window[0]):
                        # skip data that has already been added
                        continue

                    # convert the ip to a binary string
                    bin_pfx = self.rh.get_bin_pfx(prefix)
                    pipe.zadd("%s:%s" % (REDIS_P2A_PFX, bin_pfx), timestamp,
                              "%x:%s" % ((timestamp / TIME_GRANULARITY), str(new_asn)))

                    # save as2pfx data into dictionary
                    if new_asn not in as2pfx_dict:
                        as2pfx_dict[new_asn] = set()
                    as2pfx_dict[new_asn].add(prefix)

            # loop through as2pfx_dict and write them into database
            for asn in as2pfx_dict:
                pipe.zadd("%s:%s" % (REDIS_A2P_PFX, asn), file_timestamp,
                          "%x:%s" % ((file_timestamp / TIME_GRANULARITY), ",".join(as2pfx_dict[asn])))

        except swiftclient.exceptions.ClientException as e:
            logging.error("Could not read pfx-origin file '%s'" % path)
            logging.error(e.msg)
            return
        except IOError as e:
            logging.error("Could not read pfx-origin file '%s'" % path)
            logging.error("I/O error: %s" % e.strerror)
            return
        except ValueError as e:
            logging.error(e.args)
            return
        inserted = pipe.execute()
        logging.info("Inserted %d prefixes" % (len(inserted)))
        pipe.zadd(TIMESTAMPS_KEY, file_timestamp,
                  "%s:%s-pfxs" % (file_timestamp, len(inserted)))
        pipe.execute()

    def insert_pfx_timestamp(self, unix_ts, force=False):
        ts = datetime.datetime.utcfromtimestamp(unix_ts)
        swift_obj = SWIFT_OBJ_TMPL % (ts.year, ts.month, ts.day, ts.hour, unix_ts)
        swift_path = "swift://%s/%s" % (SWIFT_CONTAINER, swift_obj)
        self.insert_pfx_file(swift_path, force=force)

    @staticmethod
    def _extract_res(redis_result):
        # redis_result[0] is the value of the result
        # for pfx2as is timestamp:asns
        # for as2pfx is timestamp:pfxs
        # redis_result[1] is the corresponding timestamp for the result

        return redis_result[0].split(":")[1], int(redis_result[1])

    def lookup(self, prefix, max_ts=None, exact_match=False, latest=False):
        """
        Queries redis for pfx2as mappings for the last 24 hours
        Returns:
        - the queried prefix or closest super-prefix (if exact_match not set)
        - if a timestamp is specified, returns the most recent (ASN, timestamp)
        (before the timestamp) otherwise, a list of tuple (ASN, timestamp).

        i.e., ('8.8.8.0/24', [('15169', 1473120000.0)]
        """
        if max_ts is None:
            max_ts = "+inf"
        asns = []
        bin_pfx = self.rh.get_bin_pfx(prefix)
        if bin_pfx is None:
            return None, []
        # format in redis [timestamp, AS-timestamp]
        # in this way we can save all the timestamp
        while len(bin_pfx) > 1:
            asns = self.rh.zrangebyscore("%s:%s" % (REDIS_P2A_PFX, bin_pfx),
                                         "-inf", max_ts, withscores=True)
            if len(asns) or exact_match:
                break
            else:
                # check for a less specific prefix
                bin_pfx = bin_pfx[:-1]

        matched_pfx = self.rh.get_str_pfx(bin_pfx)
        if not len(asns):
            return None, []

        if latest or max_ts != "+inf":
            # return the most recent
            return matched_pfx, [self._extract_res(asns[-1])]
        # return the list
        return matched_pfx, [self._extract_res(res) for res in asns]

    def lookup_as(self, asn, max_ts=None, latest=False):
        """
        Queries redis for as2pfx mappings for the last 24 hours
        """
        if max_ts is None:
            max_ts = "+inf"

        results = self.rh.zrangebyscore("%s:%s" % (REDIS_A2P_PFX, asn),
                                        "-inf", max_ts, withscores=True)

        if not len(results):
            return []

        if latest or max_ts != "+inf":
            # return the most recent
            return [self._extract_res(results[-1])]
        # return the list
        return [self._extract_res(res) for res in results]


def main():
    parser = argparse.ArgumentParser(description="""
    Utilities for populating the "newcomer" pfx2as redis database.
    """)

    parser.add_argument('-f', "--file", action="store", default=None, help="pfx-origins file")

    parser.add_argument('-t', "--timestamp", action="store", default=None, help="Insert data for given timestamp")

    parser.add_argument('-r', "--redis-host", action="store", default=None, help='Redis address')

    parser.add_argument('-p', "--redis-port", action="store", default=6379, help='Redis port')

    parser.add_argument('-d', "--redis-db", action="store", help='Redis database', default=1)

    parser.add_argument('-l', "--lookup", action="store",
                        help="Look up the given prefix "
                             "(timestamp may be specified using --timestamp)")

    parser.add_argument('-L', "--lookup-as", action="store",
                        help="Look up the given AS number "
                             "(timestamp may be specified using --timestamp)")

    parser.add_argument('-e', "--exact", action="store_true", default=False,
                        help="Restrict lookups to exact matches")

    parser.add_argument('-n', "--latest", action="store_true", default=False,
                        help="Get only the most recent match")

    parser.add_argument('-w', "--window-hours", action="store", default=DEFAULT_WINDOW_HOURS,
                        help='Length of the window (hours)')

    parser.add_argument('-c', "--clean", action="store_true", default=False,
                        help="Remove data outside window")

    parser.add_argument('-s', "--show-window", action="store_true", default=False,
                        help="Show the current window")

    parser.add_argument('-m', "--missing", action="store_true", default=False,
                        help="Show missing data in the current window")

    parser.add_argument('-v', "--verbose", action="store_true", default=False,
                        help="Print debugging information")

    parser.add_argument('-o', "--overwrite", action="store_true", default=False,
                        help="Force insertion even if timestamp has already been inserted")

    opts = parser.parse_args()

    pfx2as = Pfx2AsNewcomer(
        opts.window_hours,
        opts.redis_host,
        opts.redis_port,
        opts.redis_db,
        "DEBUG" if opts.verbose else "INFO"
    )

    if opts.lookup:
        print(pfx2as.lookup(prefix=opts.lookup, max_ts=opts.timestamp,
                            exact_match=opts.exact, latest=opts.latest))
        # don't move on to any of the insertion code!
        return

    if opts.lookup_as:
        print(pfx2as.lookup_as(asn=opts.lookup_as, max_ts=opts.timestamp,
                               latest=opts.latest))
        # don't move on to any of the insertion code!
        return

    if opts.show_window:
        pfx2as.print_window_info()

    if opts.timestamp is not None:
        pfx2as.insert_pfx_timestamp(int(opts.timestamp), force=opts.overwrite)

    if opts.file is not None:
        pfx2as.insert_pfx_file(opts.file, force=opts.overwrite)

    if opts.missing:
        missing = pfx2as.find_missing_inside_window()
        for miss in missing:
            print(miss)

    if opts.clean:
        pfx2as.remove_outside_window()


if __name__ == "__main__":
    main()
