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
import logging
import os
import signal
import sys
import time

import grip.coodinator.announce
import grip.redis


def update_adj(ann):
    adj = grip.redis.Adjacencies()
    missing = adj.find_missing(int(ann.timestamp), None)
    for miss in missing:
        adj.insert_adj_timestamp(miss)


def update_pfx2as_newcomer(ann):
    pfx2as = grip.redis.Pfx2AsNewcomer()
    # insert this data
    pfx2as.insert_pfx_timestamp(int(ann.timestamp))
    # check if there is anything missing now that we have slid the window
    # 2018-08-22 AK comments because when we have a lot of data missing from
    # swift this can take too long. in case the DB is missing some data that
    # actually exists, the best option is to manually run the
    # grip-redis-pfx2as-newcomer-missing.sh script
    # missing = pfx2as.find_missing_inside_window()
    # for miss in missing:
    #     pfx2as.insert_pfx_timestamp(miss)
    # and then remove data that is now outside the window
    pfx2as.remove_outside_window()


def check_and_create_lockfile(filename):
    """check if lockfile exists, if not, create a lock file"""
    if os.path.isfile(filename):
        return False

    open(filename, "a").close()
    return True


def update_pfx2as_historical(ann):
    pfx2as = grip.redis.Pfx2AsHistorical()
    pfx2as.insert_pfx_timestamp(int(ann.timestamp))

    # comment out the following `remove_outside_window` command
    # we don't need to save space anymore, the more data the better now
    # pfx2as.remove_outside_window()
    # not much point checking for missing data...
    # TODO: rework how the historical DB works so we
    # TODO: can more easily patch small holes?


DB_TYPES = {
    "adjacencies": {
        "consumer-name": "triplets-weekly",
        "update": update_adj,
    },
    "pfx2as-newcomer": {
        "consumer-name": "pfx-origins",
        "update": update_pfx2as_newcomer,
    },
    "pfx2as-historical": {
        "consumer-name": "pfx-origins",
        "update": update_pfx2as_historical,
    }
}
DEFAULT_GROUP_TMPL = "grip-redis-production-%s"


def main():
    parser = argparse.ArgumentParser(description="""
    Simple service to listen for announcements and trigger updates of a redis
    DB.
    """)
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument('-a', "--adjacencies", action="store_true",
                     help="Update Adjacencies DB")
    grp.add_argument('-n', "--pfx2as-newcomer", action="store_true",
                     help="Update Pfx2AS-Newcomer DB")
    grp.add_argument('-p', "--pfx2as-historical", action="store_true",
                     help="Update Pfx2AS-Historical DB")

    parser.add_argument('-g', "--group", nargs="?",
                        help="Set Kafka consumer group")

    parser.add_argument('-t', '--test', action="store_true",
                        help="Use testing group")

    parser.add_argument('-v', '--verbose', action="store_true",
                        required=False, help='Verbose logging')

    opts = parser.parse_args()

    logging.basicConfig(level="DEBUG" if opts.verbose else "INFO",
                        format="%(asctime)s|%(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    historical_lockfile = "/tmp/grip-redis-pfx2as-historical.lock"

    if opts.adjacencies:
        db_type = "adjacencies"
    elif opts.pfx2as_newcomer:
        db_type = "pfx2as-newcomer"
    else:
        assert opts.pfx2as_historical
        db_type = "pfx2as-historical"
        if not check_and_create_lockfile(historical_lockfile):
            logging.error("lockfile %s exists, stop updating pfx2as historical data"
                          % historical_lockfile)
            sys.exit(1)

    if not opts.group:
        opts.group = DEFAULT_GROUP_TMPL % db_type
    if opts.test:
        opts.group = "%s-TEST" % opts.group

    shutdown = {"count": 0}

    def _stop_handler(_signo, _stack_frame):
        logging.info("Caught signal, shutting down at next opportunity")
        shutdown["count"] += 1
        if shutdown["count"] > 3:
            logging.warning("Caught %d signals, shutting down NOW" % shutdown["count"])
        if db_type == "pfx2as-historical":
            os.remove(historical_lockfile)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)

    logging.info("Starting updater for %s" % db_type)
    logging.info("Group: %s" % opts.group)

    cfg = DB_TYPES[db_type]

    listener = grip.coodinator.announce.Listener(
        offset="earliest" if opts.test else "latest",
        group=opts.group,
        sender_type="consumer",
        sender_name=cfg["consumer-name"]
    )
    for announcement in listener.listen():
        logging.debug(announcement)
        delay = time.time() - int(announcement.timestamp)
        if delay > 45 * 60:
            logging.warning("Inserting outdated info (%d)" % int(announcement.timestamp))
            os.system("/usr/local/bin/grip-slackmsg.sh redis-updater-%s "
                      "inserting outdated data: %d. delay: %ds" %
                      (db_type, int(announcement.timestamp), delay))
        cfg["update"](announcement)
        if shutdown["count"] > 0:
            logging.info("Shutting down")
            break
    if db_type == "pfx2as-historical":
        os.remove(historical_lockfile)
