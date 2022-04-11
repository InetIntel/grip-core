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
import json
import logging
import multiprocessing as mp
import os
import time
from datetime import datetime

from grip.events.event import Event
from grip.inference.inference_collector import InferenceCollector
from grip.utils.data.elastic import ElasticConn
from grip.utils.data.elastic_queries import query_in_range, query_no_inference


class InferenceRunner:
    def __init__(self, event_type, debug):
        self.event_type = event_type
        self.debug = debug
        self.collector = InferenceCollector(event_type=event_type, debug=debug)
        self.esconn = ElasticConn()

    def refill(self):
        """
        TODO: add it in cli option
        Find and refill events that have no inference results on ElasticSearch
        :return:
        """

        for event in self.esconn.search_generator(index="observatory-events-*", query=query_no_inference()):
            assert (isinstance(event, Event))
            event = self.collector.infer_event(event, to_query_hegemony=False)
            self.esconn.index_event(event, debug=self.debug)

    def rerun(self, start_ts, end_ts, tr_worthy, inserted_before=None, inserted_after=None, must_tags=None, must_not_tags=None):
        """
        Rerun the inference code for the given time period.

        :param start_ts:
        :param end_ts:
        :param tr_worthy: weather to process only traceroute-worthy event
        :param inserted_before: only process events inserted before certain time
        :param inserted_after: only process events inserted after certain time
        :return:
        """
        self._show_time(start_ts, "start")
        self._show_time(end_ts, "end")
        self._show_time(inserted_before, "inserted before")
        self._show_time(inserted_after, "inserted after")

        query = query_in_range(start_ts, end_ts, must_tr_worthy=tr_worthy, must_tags=must_tags, must_not_tags=must_not_tags)
        json.dumps(query, indent=4)
        if self.debug:
            index_pattern = "observatory-v3-test-events-{}-*".format(self.event_type)
        else:
            index_pattern = "observatory-v3-events-{}-*".format(self.event_type)

        for event in self.esconn.search_generator(index=index_pattern, query=query):
            assert (isinstance(event, Event))
            if self._event_in_range(event, inserted_before, inserted_after):
                event.summary.clear_inference()
                self.collector.infer_event(event=event, to_query_asrank=True, to_query_hegemony=False)
                self.esconn.index_event(event, debug=self.debug)

    @staticmethod
    def _event_in_range(event: Event, before, after):
        if before and event.insert_ts > before:
            return False
        if after and event.insert_ts < after:
            return False
        return True

    @staticmethod
    def _show_time(ts: int, name: str):
        if ts:
            dt_object = datetime.utcfromtimestamp(ts)
            logging.info(f"{name}: {dt_object}")


def run_process(event_type, debug, start_ts, end_ts, tr_worthy, after_ts, before_ts, must_tags, must_not_tags):
    InferenceRunner(event_type=event_type, debug=debug) \
        .rerun(start_ts, end_ts, tr_worthy, before_ts, after_ts, must_tags, must_not_tags)


def main():
    parser = argparse.ArgumentParser(
        description="Utility to listen for new events and trigger active measurements.")

    parser.add_argument('-t', "--type", nargs="?", required=True,
                        help="Event type to rerun")

    # timestamps
    parser.add_argument("-s", "--start_ts", type=int, nargs="?", required=True,
                        help="start time for rerun (unix time)")
    parser.add_argument("-e", "--end_ts", type=int, nargs="?",
                        help="end time for rerun (unix time)")
    parser.add_argument("-b", "--before_ts", type=int, nargs="?",
                        help="inserted before time")
    parser.add_argument("-a", "--after_ts", type=int, nargs="?",
                        help="inserted after time")

    # runtime
    parser.add_argument('-p', '--processes', nargs="?", type=int, required=False,
                        default=1,
                        help="Number of processes to divide the time range and run, specify 0 to use all available cores")
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Whether to enable debug mode")

    # event filters
    parser.add_argument("-w", "--tr_worthy", action="store_true", default=False,
                        help="Only process traceroute-worthy events?")
    parser.add_argument('-m', "--must_tags", nargs="?", required=False,
                        help="must have one of the tags, separated by comma")
    parser.add_argument('-M', "--must_not_tags", nargs="?", required=False,
                        help="must NOT have one of the tags, separated by comma")

    opts = parser.parse_args()

    # boost elasticsearch logging output to INFO level
    logging.getLogger('elasticsearch').setLevel(logging.INFO)
    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s", level=logging.INFO)

    if not opts.end_ts:
        opts.end_ts = int(time.time())

    must_tags = None
    must_not_tags = None
    if opts.must_tags:
        must_tags = opts.must_tags.split(",")
    if opts.must_not_tags:
        must_not_tags = opts.must_not_tags.split(",")

    processes = opts.processes
    if processes <= 0:
        processes = os.cpu_count()
    step = int((opts.end_ts - opts.start_ts) / processes)
    cur_ts = opts.start_ts
    args = []
    while cur_ts < opts.end_ts:
        cur_end = cur_ts + step
        args.append((opts.type, opts.debug, cur_ts, cur_end, opts.tr_worthy, opts.after_ts, opts.before_ts, must_tags, must_not_tags))
        cur_ts += step
    logging.info(args)

    with mp.Pool(processes=processes) as pool:
        pool.starmap(run_process, args)


if __name__ == "__main__":
    main()

