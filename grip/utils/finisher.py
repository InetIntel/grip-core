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
import time

import wandio
from joblib._multiprocessing_helpers import mp

from grip.events.event import Event
from grip.events.pfxevent_parser import PfxEventParser
from grip.tagger.finisher import Finisher
from grip.utils.fs import fs_generate_file_list, fs_get_timestamp_from_file_path


class FinisherEngine:

    def __init__(self, event_type, datadir):
        self.event_type = event_type
        self.base_datadir = datadir
        self.history_path = datadir + "/" + event_type

    def _extract_finished_event(self, filename):
        """
        One consumer file to one finished events with many finished prefix events in it.

        :param filename: file name for consumer data
        :return: one finished event corresponding to the consumer file
        """

        parser = PfxEventParser(self.event_type)
        finished_event = None
        for line in wandio.open(filename):
            # ignore commented lines
            if line.startswith("#") or "FINISHED" not in line:
                continue

            try:
                pfx_event = parser.parse_line(line)
            except ValueError as e:
                # handling event parsing error here
                logging.error("parsing pfx event failed (finished): {}".format(line))
                raise e
            if pfx_event is None:
                # skipping not recognizable pfxevent string
                continue
            assert pfx_event.position == "FINISHED"
            if finished_event is None:
                finished_event = Event.from_pfxevent(pfx_event)
            finished_event.add_pfx_event(pfx_event)
        return finished_event

    def run_finisher(self, pfx_datadir, start_ts=None, end_ts=None, debug=False):
        finisher = Finisher(event_type=self.event_type, load_unfinished=False,
                debug=debug, pfx_datadir=pfx_datadir)
        finisher.load_unfinished_events(self.event_type, start_ts=start_ts, end_ts=end_ts)
        if not finisher.unfinished_events:
            # no unfinished events
            return

        minimum_ts = int(min(finisher.unfinished_events).split("-")[1])
        maximum_ts = int(max(finisher.unfinished_events).split("-")[1]) + 3600

        # now look for all finished events from start_ts
        logging.info("start processing data from consumer files on disk")
        mustbegin = self.history_path + "/year="

        for fname in fs_generate_file_list(self.history_path):
            if not fname.startswith(mustbegin):
                continue
            ts = fs_get_timestamp_from_file_path(fname)
            if ts < minimum_ts or ts > maximum_ts:
                continue

            logging.info("backfilling events on time {} earliest {}".format(ts, minimum_ts))
            finished_event = self._extract_finished_event(fname)
            if finished_event is None:
                continue
            updated = finisher.process_finished_event(finished_event)
            if not finisher.unfinished_events:
                break
            if updated:
                # recalculate the minimum_ts if events have been updated
                minimum_ts = int(min(finisher.unfinished_events).split("-")[1])


def run_process(event_type, start_ts, end_ts, datadir, debug, pfx_datadir):
    finisher = FinisherEngine(event_type, datadir)
    finisher.run_finisher(pfx_datadir, start_ts=start_ts, end_ts=end_ts, debug=debug)


def main():
    parser = argparse.ArgumentParser(
        description="Utility to listen for new events and trigger active measurements.")

    parser.add_argument('-t', "--type", nargs="?", required=True,
                        help="Event type to listen for")
    parser.add_argument("-s", "--start_ts", type=int, nargs="?", required=True,
                        help="start time for rerun (unix time)")
    parser.add_argument("-e", "--end_ts", type=int, nargs="?",
                        help="end time for rerun (unix time)")
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Run in debug mode -- events will be read and re-written to -test- indices")
    parser.add_argument("-D", "--datadir", type=str,
                        default="/data/bgp/historical",
                        help="Base directory to search for historical consumer data with unfinished events")
    parser.add_argument("-P", "--pfx-datadir", type=str,
                        default="/data/bgp/historical",
                        help="Base directory to search for pfx-origins data")
    parser.add_argument('-p', '--processes', nargs="?", type=int, required=False,
                        default=1,
                        help="Number of processes to divide the time range and run, specify 0 to use all available cores")


    opts = parser.parse_args()
    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                        # filename=LOG_FILENAME,
                        level=logging.INFO)

    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                        level=logging.INFO)
    logging.getLogger('elasticsearch').setLevel(logging.INFO)

    if not opts.end_ts:
        opts.end_ts = int(time.time())

    processes = opts.processes
    if processes == 1:
        # single-process runner
        run_process(opts.type, opts.start_ts, opts.end_ts, opts.datadir,
                opts.debug, opts.pfx_datadir)
        return

    # multi-process runner
    if processes <= 0:
        processes = os.cpu_count()
    step = int((opts.end_ts - opts.start_ts) / processes)
    cur_ts = opts.start_ts
    args = []
    while cur_ts < opts.end_ts:
        cur_end = cur_ts + step
        args.append((opts.type, cur_ts, cur_end, opts.datadir, opts.debug,
                opts.pfx_datadir))
        cur_ts += step
    logging.info(args)

    with mp.Pool(processes=processes) as pool:
        pool.starmap(run_process, args)


if __name__ == "__main__":
    main()

