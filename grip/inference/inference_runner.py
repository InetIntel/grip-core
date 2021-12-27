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
import hashlib
import json
import logging
import multiprocessing as mp
import os
from datetime import datetime

import elasticsearch
import filelock as filelock

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
            self.esconn.index_event(event)

    def rerun(self, start_ts, end_ts, tr_worthy, inserted_before=None, inserted_after=None,
              must_tags=None, must_not_tags=None, missing_inference=False, missing_data=False, query_asrank=False):
        """
        Rerun the inference code for the given time period.

        :param query_asrank:
        :param missing_data:
        :param missing_inference:
        :param must_not_tags:
        :param must_tags:
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

        query = query_in_range(start_ts, end_ts, must_tr_worthy=tr_worthy,
                               must_tags=must_tags, must_not_tags=must_not_tags, missing_inference=missing_inference, missing_data=missing_data)
        if missing_data:
            # if we want to fix missing data, force query_asrank to True
            query_asrank=True

        json.dumps(query, indent=4)
        type_pattern = self.event_type
        if type_pattern is None:
            type_pattern = "*"
        assert (type_pattern in ["*", "moas", "submoas", "defcon", "edges"])
        for event in self.esconn.search_generator(index=self.esconn.get_index_name(type_pattern), query=query):
            try:
                assert (isinstance(event, Event))
                if self._event_in_range(event, inserted_before, inserted_after):
                    event.summary.clear_inference()
                    self.collector.infer_event(event=event, to_query_asrank=query_asrank, to_query_hegemony=False)
                    self.esconn.index_event(event)
            except elasticsearch.exceptions.TransportError as error:
                if error.status_code == 413:
                    # allow continuing the program if certain events are too large
                    logging.warning("event is too large to recommit: {}".format(event.event_id))
                else:
                    raise error

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
            logging.info("{}: {}".format(name, dt_object))


def run_process(event_type, debug, start_ts, end_ts, tr_worthy, after_ts, before_ts, must_tags, must_not_tags,
                missing_inference, missing_data, query_asrank):
    InferenceRunner(event_type=event_type, debug=debug) \
        .rerun(start_ts, end_ts, tr_worthy, before_ts, after_ts, must_tags, must_not_tags, missing_inference, missing_data, query_asrank)


def main():
    parser = argparse.ArgumentParser(
        description="Utility to listen for new events and trigger active measurements.")

    parser.add_argument('-t', "--type", nargs="?", required=False,
                        help="Event type to listen for")

    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                        # filename=LOG_FILENAME,
                        level=logging.INFO)

    # Flags
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Whether to enable debug mode")
    parser.add_argument("-w", "--tr_worthy", action="store_true", default=False,
                        help="Only process traceroute-worthy events?")
    parser.add_argument("-A", "--as_rank", action="store_true", default=False,
                        help="Whether to query ASRank")
    parser.add_argument("-f", "--fix_missing", action="store_true", required=False, default=False,
                        help="fix missing inference objects")
    parser.add_argument("-F", "--fix_data", action="store_true", required=False, default=False,
                        help="fix missing external data (ASRank)")

    # Values
    parser.add_argument("-s", "--start_ts", type=int, nargs="?", required=False,
                        help="start time for rerun (unix time)")
    parser.add_argument("-e", "--end_ts", type=int, nargs="?",
                        help="end time for rerun (unix time)")
    parser.add_argument("-b", "--before_ts", type=int, nargs="?",
                        help="inserted before time")
    parser.add_argument("-a", "--after_ts", type=int, nargs="?",
                        help="inserted after time")
    parser.add_argument('-p', '--processes', nargs="?", type=int, required=False,
                        default=1,
                        help="Number of processes to divide the time range and run, specify 0 to use all available cores")
    parser.add_argument('-m', "--must_tags", nargs="?", required=False,
                        help="must have one of the tags, separated by comma")
    parser.add_argument('-M', "--must_not_tags", nargs="?", required=False,
                        help="must NOT have one of the tags, separated by comma")

    opts = parser.parse_args()
    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                        # filename=LOG_FILENAME,
                        level=logging.INFO)

    logging.getLogger('elasticsearch').setLevel(logging.INFO)

    must_tags = None
    must_not_tags = None
    if opts.must_tags:
        must_tags = opts.must_tags.split(",")
    if opts.must_not_tags:
        must_not_tags = opts.must_not_tags.split(",")

    processes = opts.processes
    if processes <= 0:
        processes = os.cpu_count()
    if processes > 1 and not (opts.end_ts and opts.start_ts):
        logging.info(
            "cannot start multi-threading due to lack of end_ts or start_ts, forced back to single-thread processing")
        processes = 1

    hash_str = hashlib.sha1((json.dumps(vars(opts), sort_keys=True, ensure_ascii=True)).encode()).hexdigest()
    lockfile = "/tmp/inference-runner-{}.lock".format(hash_str)
    lock = filelock.FileLock(lockfile)
    with lock.acquire(timeout=1):
        if processes == 1:
            logging.info("single-threaded processing starts...")
            run_process(opts.type, opts.debug,
                        opts.start_ts, opts.end_ts, opts.tr_worthy, opts.after_ts, opts.before_ts,
                        must_tags, must_not_tags, opts.fix_missing, opts.fix_data, opts.as_rank)
        else:
            args = []
            step = int((opts.end_ts - opts.start_ts) / processes)
            cur_ts = opts.start_ts
            while cur_ts < opts.end_ts:
                cur_end = cur_ts + step
                args.append(
                    (opts.type, opts.debug, cur_ts, cur_end, opts.tr_worthy, opts.after_ts, opts.before_ts, must_tags,
                     must_not_tags, opts.fix_missing, opts.fix_data, opts.as_rank))
                cur_ts += step

            with mp.Pool(processes=processes) as pool:
                pool.starmap(run_process, args)

    # remove lockfile if process finishes running
    if os.path.exists(lockfile):
        os.remove(lockfile)


if __name__ == "__main__":
    main()
