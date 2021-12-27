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
import logging
import re
import sys
import time

import swiftclient
import wandio

from grip.redis.redis_helper import RedisHelper

# -- MAIN DB KEYS --
# main mapping from prefix to ASes (and most recent timestamp)
PFX_KEY_TMPL = "PFX:HIST:IPV4:%s"
# days that are fully inserted in the DB
DAYS_KEY = "PFX:HIST:IPV4:DAYS"

# -- WIP DB KEYS --
# the day (midnight timestamp) current being inserted
WIP_DAY_KEY = "PFX:HIST:WIP:IPV4"
# set of 5min timestamps that have been inserted for the current day
WIP_TS_KEY = "PFX:HIST:WIP:IPV4:TS"
# the duration for each pfx/AS mapping for the WIP day
WIP_PFX_KEY_TMPL = "PFX:HIST:WIP:IPV4:PFX:%s"

# min time a pfx must be announced by an AS in one day to be
# considered "announced" for that day
MIN_DAILY_DURATION = 6 * 3600

# TODO: consider abstracting some of this code that is common to newcomer
PFX_ORIGINS_DATA_DIRECTORY = "/data/bgp/live/pfx-origins/production"
PFX_ORIGINS_FILE_NAME_TMPL = "year=%04d/month=%02d/day=%02d/hour=%02d/pfx-origins.%d.gz"
TIME_GRANULARITY = 300
DEFAULT_WINDOW_DAYS = 365


class Pfx2AsHistorical:

    def __init__(self, host=None, port=6379, db=0, log_level="INFO"):
        if host is not None:
            self.rh = RedisHelper(host, port, db, log_level)
        else:
            self.rh = RedisHelper(port=port, db=db, log_level=log_level)

    def get_inserted_days(self):
        return set([int(ts[1]) for ts in
                    self.rh.zrange(DAYS_KEY, 0, -1, withscores=True)])

    def get_current_window(self):
        day_tses = self.get_inserted_days()
        if not len(day_tses):
            return None, None, None
        return min(day_tses), max(day_tses), set(day_tses)

    def find_missing(self, window_days=None):
        """
        Find missing days of data for the historical prefix-to-asn database.
        Technically, the database can hold data indifinitely, but we still check days back to now - window_days before.

        :param window_days: The number of days it should go back to
        :return:
        """
        begin_ts, end_ts, all_tses = self.get_current_window()
        today = int(time.time() / 86400) * 86400
        if window_days is None:
            now = begin_ts
        else:
            now = today - (window_days * 86400)
        missing = []
        while now < today:
            # check for all days within the past window that has not been inserted
            if begin_ts is None \
                    or now < begin_ts \
                    or now > end_ts \
                    or now not in all_tses:
                missing.append(now)
            now += 86400
        return missing

    def print_window_info(self):
        begin_ts, end_ts, all_tses = self.get_current_window()
        print("Current window: [%s, %s]" % (begin_ts, end_ts))

    def _get_wip_day(self):
        wip_day = self.rh.get(WIP_DAY_KEY)
        return int(wip_day) if wip_day is not None else None

    def _get_wip_ts(self):
        return set(self.rh.smembers(WIP_TS_KEY))

    def insert_pfx_file(self, path, ts=None, force_promote=False, disable_promote=False):
        if ts is None:
            # FIXME: this is fragile and should be fixed
            ts = int(path.split(".")[1])

        if ts % TIME_GRANULARITY != 0:
            logging.error("Input timestamp (%d) is not at %d second granularity"
                          % (ts, TIME_GRANULARITY))
            return

        logging.info("Inserting pfx2as mappings from %s" % path)

        # what day is this?
        day_ts = int(ts / 86400) * 86400

        # what is the current WIP day?
        wip_day_ts = self._get_wip_day()

        # can we insert data for this day?
        if not disable_promote and wip_day_ts is not None and wip_day_ts != day_ts:
            logging.warning("Inserting data for %d but WIP day is %d. "
                            "Promoting and clearing WIP data." %
                            (day_ts, wip_day_ts))
            self.promote_wip()
            self.clean_wip()
            wip_day_ts = None

        # if the current WIP day is unset, then set it
        if wip_day_ts is None:
            self.rh.set(WIP_DAY_KEY, day_ts)
            wip_day_ts = self._get_wip_day()
        assert wip_day_ts == day_ts

        # we know our day is the same as the WIP day, but has this file already
        # been inserted?
        if self.rh.sismember(WIP_TS_KEY, ts):
            logging.error("WIP data for %d already inserted" % ts)
            logging.error("Run with the `--clean` option to remove WIP data")
            return

        # now add to the WIP TSes (this way if we crash there is a record)
        self.rh.sadd(WIP_TS_KEY, ts)

        # ok, we're good to go

        pipe = self.rh.get_pipeline()
        # insert file into DB
        try:
            with wandio.open(path) as fh:
                for line in fh:
                    # 1476104400|115.116.0.0/16|4755|4755|STABLE
                    timestamp, prefix, old_asn, new_asn, label = line.strip().split("|")
                    if label != "STABLE" or ":" in prefix:
                        # do not insert prefixes that are not stable
                        #   (since we're looking for 6 hours of stability)
                        # we also do not (currently) support IPv6 prefixes
                        continue
                    if not re.match("^[0-9 ]+$", new_asn):
                        continue
                    bin_pfx = self.rh.get_bin_pfx(prefix)
                    # update the duration for this pfx/asn combo
                    pipe.zincrby(WIP_PFX_KEY_TMPL % bin_pfx, new_asn,
                                 TIME_GRANULARITY)
        except IOError as e:
            logging.error("Could not read pfx-origin file '%s'" % path)
            logging.error("I/O error: %s" % e.strerror)
            return
        res = pipe.execute()
        logging.info("Inserted %d pfx2as mappings into %s" %
                     (len(res), WIP_PFX_KEY_TMPL % "*"))

        if force_promote:
            self.promote_wip()

    # TODO: consider moving this to the helper class
    def insert_pfx_timestamp(self, unix_ts, promote=False, disable_promote=False):
        ts = datetime.datetime.utcfromtimestamp(unix_ts)
        # swift_obj = SWIFT_OBJ_TMPL % (ts.year, ts.month, ts.day, ts.hour, unix_ts)
        # swift_path = "swift://%s/%s" % (SWIFT_CONTAINER, swift_obj)
        data_file_name = PFX_ORIGINS_FILE_NAME_TMPL % (ts.year, ts.month, ts.day, ts.hour, unix_ts)
        data_file_path = "%s/%s" % (PFX_ORIGINS_DATA_DIRECTORY, data_file_name)
        self.insert_pfx_file(data_file_path, force_promote=promote, disable_promote=disable_promote)

    def fix_ranges(self):
        """
        fix ranges for records. there were records with continuous time ranges that were saved separately due to a bug
        in the promoting procedure. this function goes through all records and re-organize the data to make sure the
        continuous range were saved as one range.
        """

        pipe = self.rh.get_pipeline()
        count = 0
        for key in self.rh.scan_keys(PFX_KEY_TMPL % "*"):
            if "DAYS" in key:
                continue
            count += 1
            toadd = []
            # scan for all keys
            prev_ts = 0
            for record, score in self.rh.zrangebyscore(key, "-inf", "+inf", withscores=True):
                end_ts = int(score)
                # print out the match
                if ":" not in record:
                    logging.error("empty record '%s'" % record)
                    continue

                ts, asns = record.split(":")
                start_ts = int(ts)
                assert (start_ts <= end_ts)

                if prev_ts == 0:
                    # if first time
                    toadd.append((start_ts, end_ts, asns))
                else:
                    if prev_ts == start_ts - 86400:
                        # if the two ranges are continuous
                        t1, t2, asns_str = toadd.pop()
                        if asns_str == asns:
                            # the previouse origins are the same as the current one
                            toadd.append((t1, end_ts, asns_str))
                        else:
                            # push it right back
                            toadd.append((t1, t2, asns_str))
                    else:
                        toadd.append((start_ts, end_ts, asns))
                prev_ts = end_ts

            pipe.delete(key)
            if toadd:
                cache = []
                for (ts_start, ts_end, asns_str) in toadd:
                    cache.append(ts_end)
                    cache.append("{}:{}".format(ts_start, asns_str))
                pipe.zadd(key, *cache)
            if count % 1000 == 0:
                res = pipe.execute()
                logging.info("count %d: fixed ranges for %d pfx/AS mappings in main DB" % (count, len(res) / 2))

    def promote_wip(self):
        wip_day_ts = self._get_wip_day()
        if wip_day_ts is None:
            logging.error("No WIP data")
            return

        # get the WIP timestamps
        wip_tses = self._get_wip_ts()

        if len(wip_tses) != 86400 / TIME_GRANULARITY:
            logging.warning("Only %d/%d WIP timestamps present. Promoting anyway." %
                            (len(wip_tses), 86400 / TIME_GRANULARITY))

        logging.info("Promoting WIP data for %d" % wip_day_ts)

        pipe = self.rh.get_pipeline()
        # for each WIP prefix, find the ASes that have a
        # duration >= MIN_DAILY_DURATION
        for key in self.rh.scan_keys(WIP_PFX_KEY_TMPL % "*"):
            bin_pfx = key.split(":")[-1]
            toadd = []
            cache = []
            for asn in self.rh.zrangebyscore(key, MIN_DAILY_DURATION, "+inf"):
                if not re.match("^[0-9 ]+$", asn):
                    continue
                asns = asn.split(" ")
                cache.append((wip_day_ts, asns))
            # insert this ASN/day in the main DB
            for wip_day_ts, asns in cache:
                records = self.rh.zrangebyscore(PFX_KEY_TMPL % bin_pfx,
                                                "-inf", "+inf", withscores=True)
                # records = list(map(lambda (x, score): (x.split(":"), str(int(score))), records))
                # TODO: test the line below
                records = [(x.split(":"), str(int(score))) for x, score in records]
                processed = False
                for (ts, asns_str), score in records:
                    asns2 = asns_str.split(" ")
                    if asns == asns2:
                        # if the origin list is the same, there is not change in owner ship
                        if int(score) == int(wip_day_ts) - 86400:
                            # if there is a record ended before the current day,
                            # extend it by just writing the data as is with the score = wip_day_ts
                            toadd.append(wip_day_ts)  # reset the ending time as score
                            toadd.append("{}:{}".format(ts, " ".join(asns)))
                            processed = True
                            break
                        if int(ts) == int(wip_day_ts) - 86400:
                            # if the record started right after the current day
                            toadd.append(score)  # keep the score for ending time
                            toadd.append("{}:{}".format(wip_day_ts, " ".join(asns)))
                            processed = True
                            break
                        if int(ts) <= int(wip_day_ts) <= int(score):
                            # if the record falls in an existing range (shouldn't happen)
                            processed = True
                            break

                if not processed:
                    # if the record is a brand-new one, write out as is
                    toadd.append(wip_day_ts)  # reset the ending time as score
                    toadd.append("{}:{}".format(wip_day_ts, " ".join(asns)))

            if len(toadd):
                pipe.zadd(PFX_KEY_TMPL % bin_pfx, *toadd)
        res = pipe.execute()
        logging.info("Promoted %d pfx/AS mappings to main DB" % len(res))

        # now insert the day ts into the list of days that are in the main DB
        self.rh.zadd(DAYS_KEY, wip_day_ts, wip_day_ts)

    def clean_wip(self):
        pipe = self.rh.get_pipeline()
        for key in self.rh.scan_keys(WIP_PFX_KEY_TMPL % "*"):
            pipe.delete(key)
        pipe.delete(WIP_TS_KEY)
        pipe.delete(WIP_DAY_KEY)
        res = pipe.execute()
        logging.info("Deleted %d WIP keys" % len(res))

    def dump(self, ts):

        ts = int(ts / 86400) * 86400
        for key in self.rh.scan_keys(PFX_KEY_TMPL % "*"):
            # scan for all keys
            bin_pfx = key.split(":")[-1]
            for asn in self.rh.zrangebyscore(key, ts, ts):
                # print out the match
                prefix = self.rh.get_str_pfx(bin_pfx)
                print("%s\t%s" % (prefix, asn))

    def lookup(self, prefix, min_ts=None, max_ts=None, exact_match=False):
        """
        Query Redis for historical pfx to AS mapping

        @return
        - the announced prefix or super-prefix
        - list of tuple (start_ts, end_ts, ASNS).
          Start_ts and end_ts represents the start and the end of the range of time
          we observed the prefix announced by that ASN within.

        Example:
            pfx2as_historical.py -r 10.250.0.3 -L 8.8.8.0/24 -t 1516147200 -T 1520380801
            ('8.8.8.0/24', [('1516147200', '1520380800', ['15169'])])
        """
        if min_ts is None:
            min_ts = "-inf"

        bin_pfx = self.rh.get_bin_pfx(prefix)
        records = []
        while len(bin_pfx) > 1:
            records = self.rh.zrangebyscore(PFX_KEY_TMPL % bin_pfx,
                                            min_ts, "+inf", withscores=True)
            # records = list(map(lambda (x, score): (x.split(":"), str(int(score))), records))
            records = [(x.split(":"), str(int(score))) for (x, score) in records]
            if len(records) or exact_match:
                break
            else:
                # checking for a less specific prefix
                bin_pfx = bin_pfx[:-1]

        if max_ts is not None:
            # filter based on starting timestamp
            # only retain records with start time before the `max_ts`
            records = [((start_ts, asns), end_ts) for ((start_ts, asns), end_ts) in records if
                       int(start_ts) <= int(max_ts)]

        # note that `asns` is a " " separated string containing a list of origins
        return self.rh.get_str_pfx(bin_pfx), [(start_ts, end_ts, asns.split(" ")) for (start_ts, asns), end_ts in
                                              records]

    @staticmethod
    def _compress_to_ranges(records):
        """
        compress a list of records, each contains a timestamp and a list of origins,
        to a list of continuous ranges
        :param records:
        :return: [score1, packed_data1, score2, packed_data2, ...]
        """

        first_record = True
        start_ts = 0
        end_ts = 0
        prev_ts = 0
        tmp_asns = []
        ranges = []
        for ts, asns in records:
            if first_record:
                # first record
                first_record = False
                start_ts = ts
                end_ts = ts
                prev_ts = ts
                tmp_asns = asns
                continue

            # compare the asns
            if asns == tmp_asns and int(ts) <= int(prev_ts) + 86400 + 100:
                # if the asns are the same and the timestamp is continuous
                # extend the range
                end_ts = ts
            else:
                ranges.append((start_ts, end_ts, tmp_asns))  # add range
                # reset the timestamps
                tmp_asns = asns  # update the stored asns
                start_ts = ts
                end_ts = ts
            prev_ts = ts

        # handle the last one
        ranges.append((start_ts, end_ts, tmp_asns))

        # tranform the ranges to redis entries
        cache = []
        for start_ts, end_ts, asns in ranges:
            cache.append(end_ts)  # end_ts as the score
            # cache.append(self.pack_data(start_ts, asns))
            cache.append("{}:{}".format(start_ts, " ".join(asns)))

        return cache


def main():
    parser = argparse.ArgumentParser(description="""
    Utilities for populating the "history" pfx2as redis database.

    Care must be taken when running multiple instances of this script.
    To batch insert data, parallelize insertion for one calendar day at a time
    (without setting the --promote option), and once insertion for one day is 
    complete, run a single-threaded promote command.

    For normal real-time insertion, run a single insertion instance with 
    the --promote option set.

    Take care not to interleave or parallelize insertion of multiple days as
    the code is configured to automatically clean the WIP data when inserting a 
    new day.

    Manual WIP cleaning is not needed for normal operations since WIP data will
    be automatically cleared when switching to a new day.
    """)

    parser.add_argument('-f', "--file", action="store", default=None,
                        help="pfx-origins file")

    parser.add_argument('-t', "--timestamp", action="store", default=None,
                        help="Insert data for given timestamp")

    parser.add_argument('-T', "--timestamp-max", action="store", default=None,
                        help="Maximum timestamp for lookup")

    parser.add_argument('-r', "--redis-host", action="store",
                        default=None, help='Redis address')

    parser.add_argument('-p', "--redis-port", action="store", default=6379,
                        help='Redis port')

    parser.add_argument('-d', "--redis-db", action="store",
                        help='Redis database', default=0)

    parser.add_argument('-D', "--dump", action="store_true", default=False,
                        help="Dump data for specific day "
                             "(timestamp should be specified using --timestamp)")

    parser.add_argument('-l', "--lookup", action="store",
                        help="Look up the given prefix "
                             "(timestamp may be specified using --timestamp and --timestamp-max)")

    parser.add_argument('-H', "--human", action="store_true", default=False,
                        help="Human readable lookup format")

    parser.add_argument('-e', "--exact", action="store_true", default=False,
                        help="Restrict lookups to exact matches")

    parser.add_argument('-s', "--show-window", action="store_true",
                        default=False, help="Show the current window")

    parser.add_argument('-m', "--missing", action="store_true",
                        default=False, help="List (day) timestamps missing")

    parser.add_argument('-w', "--window-days", action="store",
                        default=DEFAULT_WINDOW_DAYS,
                        help="Length of the window (in days)")

    parser.add_argument('-c', "--clean-wip", action="store_true", default=False,
                        help="Clean WIP data (Not needed for normal operations)")

    parser.add_argument('-x', "--clean", action="store_true", default=False,
                        help="Clean data outside of the window")

    parser.add_argument('-F', "--fix-ranges", action="store_true", default=False,
                        help="Fix ranges of data entries")

    parser.add_argument('-u', "--promote-wip", action="store_true",
                        default=False, help="Promote WIP data into main DB")
    parser.add_argument('-U', "--disable-promote", action="store_true",
                        default=False, help="Disable automatic promoting of WIP data")

    parser.add_argument('-v', "--verbose", action="store_true", default=False,
                        help="Print debugging information")

    opts = parser.parse_args()

    pfx2as = Pfx2AsHistorical(
        opts.redis_host,
        opts.redis_port,
        opts.redis_db,
        "DEBUG" if opts.verbose else "INFO"
    )

    if opts.dump:
        if opts.timestamp is None:
            parser.print_help(sys.stderr)
            return
        pfx2as.dump(int(opts.timestamp))
        return

    if opts.lookup:
        pfx_str, history = pfx2as.lookup(prefix=opts.lookup,
                                         min_ts=opts.timestamp,
                                         max_ts=opts.timestamp_max,
                                         exact_match=opts.exact,
                                         )
        if opts.human:
            print("prefix: {}".format(pfx_str))
            print("origin history:")
            for start_ts, end_ts, origins in history:
                start_str = datetime.datetime.utcfromtimestamp(int(start_ts)).strftime("%Y-%m-%d")
                end_str = datetime.datetime.utcfromtimestamp(int(end_ts)).strftime("%Y-%m-%d")
                print("{} to {}: {}".format(start_str, end_str, ",".join(origins)))
        else:
            print(pfx_str, history)
        # don't move on to any of the insertion code!
        return

    if opts.show_window:
        pfx2as.print_window_info()

    if opts.missing:
        # missing = pfx2as.find_missing(window_days=int(opts.window_days))
        missing = pfx2as.find_missing()
        for miss in missing:
            if opts.human:
                print(datetime.datetime.utcfromtimestamp(miss).strftime("%Y-%m-%d"))
            else:
                print(miss)

    if opts.timestamp is not None:
        pfx2as.insert_pfx_timestamp(int(opts.timestamp),
                                    promote=opts.promote_wip)

    if opts.file is not None:
        pfx2as.insert_pfx_file(opts.file, force_promote=opts.promote_wip, disable_promote=opts.disable_promote)

    if opts.promote_wip:
        pfx2as.promote_wip()

    if opts.clean_wip:
        pfx2as.clean_wip()

    if opts.fix_ranges:
        pfx2as.fix_ranges()

    # NOTE: do not need to remove outside window anymore due to the new effective compression scheme
    # if opts.clean:
    #     pfx2as.remove_outside_window(window_days=int(opts.window_days))


if __name__ == "__main__":
    main()
