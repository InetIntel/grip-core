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

"""
Backfilling using consumer data
"""

import argparse
import json
import logging
import multiprocessing
import multiprocessing as mp
import os
from datetime import datetime

from grip.events.details_submoas import SubmoasDetails
from grip.events.event import Event
from grip.inference.inference_collector import InferenceCollector
from grip.tagger.tagger_defcon import DefconTagger
from grip.tagger.tagger_edges import EdgesTagger
from grip.tagger.tagger_moas import MoasTagger
from grip.tagger.tagger_submoas import SubMoasTagger
from grip.tagger.tags import tagshelper
from grip.utils.data.elastic import ElasticConn
from grip.utils.data.elastic_queries import query_in_range
from grip.utils.fs import fs_generate_file_list, fs_get_timestamp_from_file_path

CLASSIFIERS = {
    "defcon": DefconTagger,
    "edges": EdgesTagger,
    "moas": MoasTagger,
    "submoas": SubMoasTagger,
}


def origins_match_paths(event: Event):
    if event.event_type != "submoas":
        return True
    for pfxevent in event.pfx_events:
        details = pfxevent.details
        assert isinstance(details, SubmoasDetails)
        sub_matches = details.get_sub_origins() == {p[-1] for p in details.get_sub_aspaths()}
        super_matches = details.get_super_origins() == {p[-1] for p in details.get_super_aspaths()}
        if not sub_matches or not super_matches:
            return False
    return True


class BackfillEngine:
    def __init__(self,
                 event_type,
                 do_traceroutes,
                 start,
                 end,
                 enable_finisher,
                 debug,
                 datadir,
                 pfxdir,
                 rpkidir,
                 ):

        # backfill configs
        self.event_type = event_type
        self.do_traceroutes = do_traceroutes
        self.start = start
        self.end = end
        self.enable_finisher = enable_finisher
        self.debug = debug
        self.history_path = datadir + "/" + event_type
        self.base_datadir = datadir
        self.pfx_datadir = pfxdir
        self.rpki_datadir = rpkidir

        # helpers
        # self.swift = SwiftUtils()
        self.es_conn = ElasticConn()
        self.inference_collector = InferenceCollector()

    def _get_parameters(self):
        return {
            "event_type": self.event_type,
            "enable_finisher": self.enable_finisher,
            "do_traceroutes": self.do_traceroutes,
            "start": self.start,
            "end": self.end,
            "debug": self.debug,
        }

    def backfill_with_consumer_data(self):
        """
        Run backfill using consumer data.

        # Step 1: gather all consumer-generated result files

        Since we do not rely on Kafka messages anymore, we need to first gather a list of consumer files that fit within
        the user-specified time range.

        # Step 2: call tagger for each consumer file

        This requires minimum changes.
        Note:
        - update tagger to allow specifying not passing data to active driver
        - make sure to use `in-memory` mode for redis data checking since we don't have newcomer data after one day old
        - make sure that inference can handle the load, and we don't do anything heavy at the inference phase

        :return:
        """
        assert (self.start is not None)

        tagger = CLASSIFIERS[self.event_type](options={
            #"offsite_mode": True,
            #"output_file": "/home/salcock3/shanetest-" + str(self.start),
            "pfx2as_file": None,
            "pfx_origins_path": self.pfx_datadir + "/pfx-origins",
            "rpki_data_dir": self.rpki_datadir + "/rpki/roas",
            "in_memory_data": True,
            "enable_finisher": self.enable_finisher,
            "force_process_view": True,  # make sure not to skip views that are already processed
            "load_unfinished": False,  # do not load unfinished events at the beginning
            "produce_kafka_message": False,  # do not produce kafka messages, i.e. disable active-probing and inference
            "debug": self.debug
        })

        # log all rerun parameters
        logging.info(self._get_parameters())

        # gather all consumer data files on swift first
        toprocess_files = []
        cache_files = []

        mustbegin = self.history_path + "/year="

        for fname in fs_generate_file_list(self.history_path):
            # only use files from the "year=XXXX/month=XX/day=XX" hierarchy
            if not fname.startswith(mustbegin):
                continue


            ts = fs_get_timestamp_from_file_path(fname)

            if ts < self.start:
                if ts >= self.start - tagger.window.window_size:
                    cache_files.append(fname)
                continue
            if self.end and ts > self.end:
                continue
            toprocess_files.append(fname)

        toprocess_files.sort()
        cache_files.sort()

        logging.info("caching total of %d consumer files" % len(cache_files))
        for fn in cache_files:
            tagger.cache_consumer_file(fn)

        logging.info("processing total of %d consumer files" % len(toprocess_files))
        # let tagger process each consumer file one by one
        for fn in toprocess_files:
            ts = fs_get_timestamp_from_file_path(fn)
            logging.info("backfilling events on time {} using {}".format(ts, fn))
            tagger.process_consumer_file(fn)


def find_unretagged_timerange(event_type, start_ts, end_ts, modified_after):
    """
    Find all timestamps that the retagging/backfill has not processed based on the search timerange and the cutoff time.
    Any events on ElasticSearch that was inserted before the cutoff time will be considered old events and be recorded
    here in the function.

    :param end_ts:
    :param start_ts:
    :param event_type:
    :param cutoff_ts:
    :return:
    """
    esconn = ElasticConn()

    query = query_in_range(start_ts, end_ts, size=1000)
    query["_source"] = ["view_ts", "last_modified_ts", "summary.tags"]  # we only want timestamps
    query["sort"] = [
        "view_ts"
    ]
    index_pattern = ElasticConn.get_index_name()
    all_event_tses = set()
    old_event_tses = set()
    last_ts = 0
    for event in esconn.search_generator(index=index_pattern, query=query, raw_json=True, timeout='1m'):
        assert "recurring-pfx-event" not in event["summary"]["tags"]
        view_ts = event["view_ts"]
        if view_ts != last_ts:
            dt_object = datetime.fromtimestamp(view_ts)
            print(dt_object)
        last_ts = view_ts
        all_event_tses.add(event["view_ts"])
        if event["last_modified_ts"] < modified_after:
            old_event_tses.add(event["view_ts"])

    all_event_tses = list(all_event_tses)
    all_event_tses.sort()
    with open("%s_event_retag_tses.csv" % event_type, "w") as of:
        of.write("timestamp,processed\n")
        for ts in all_event_tses:
            processed = "Y"
            if ts in old_event_tses:
                processed = "N"
            of.write("%d,%s\n" % (ts, processed))


def process(t, traceroutes, start, end, enable_finisher, debug, elastic,
        datadir, pfxdir, rpkidir):
    """
    Parallel processing main thread. It creates a BackfillEngine instance and start processing.
    :param t: Event type
    :param traceroutes: whether to do traceroutes
    :param start: start timestamp
    :param end: end timestamp
    :param enable_finisher: whether to enable finisher
    :param debug: whether to enable debug mode
    :param datadir: directory to search for input files to process
    :param pfxdir: directory to search for pfx-origin files
    :param rpkidir: directory to search for RPKI files
    :return:
    """
    engine = BackfillEngine(t, traceroutes, start, end, enable_finisher, debug,
            datadir, pfxdir, rpkidir)
    engine.backfill_with_consumer_data()


class Consumer(multiprocessing.Process):
    def __init__(self, event_type, task_queue, only_inference=False, debug=False):
        multiprocessing.Process.__init__(self)
        self.event_type = event_type
        self.task_queue = task_queue
        self.debug = debug
        self.only_inference = only_inference
        self.tagger = CLASSIFIERS[event_type](options={
            "pfx_origins_file": None,
            "in_memory_data": True,
            "enable_finisher": False,
            "force_process_view": True,  # make sure not to skip views that are already processed
            "load_unfinished": False,  # do not load unfinished events at the beginning
            "produce_kafka_message": False,  # do not produce kafka messages, i.e. disable active-probing and inference
            "debug": self.debug
        })
        self.inference_collector = InferenceCollector()
        self.es_conn = ElasticConn()

    def run(self):
        proc_name = self.name
        TagRecurring = tagshelper.get_tag("recurring-pfx-event")
        while True:
            event = self.task_queue.get()
            if event is None:
                # Poison pill means shutdown
                logging.info('{}: Exiting'.format(proc_name))
                self.task_queue.task_done()
                break
            assert isinstance(event, Event)
            logging.info('{}: {}'.format(proc_name, event.event_id))

            # skip recurring event retag
            if event.summary.has_tag(TagRecurring):
                logging.info("{}: skipping recurring event".format(proc_name))
                self.task_queue.task_done()
                continue
            # sanity check event
            if not origins_match_paths(event):
                logging.warning("{}: origins does not match aspaths: event_id = {}".format(proc_name, event.event_id))
                self.es_conn.delete_event_by_id(event.event_id)
                self.task_queue.task_done()
                continue

            # re-tag
            event.summary.clear_inference()

            # tag events
            if not self.only_inference:
                ts = event.view_ts
                self.tagger.update_datasets(ts)  # NOTE: only edges run special function to update dataset
                self.tagger.methodology.prepare_for_view(ts)
                self.tagger.tag_event(event)

            # re-inference
            self.inference_collector.infer_event(event=event, to_query_asrank=True, to_query_hegemony=False)

            # upload back to elastic search
            self.es_conn.index_event(event, debug=self.debug)

            # mark task as done.
            self.task_queue.task_done()


def process_elastic():
    pass


def main():
    parser = argparse.ArgumentParser(
        description="Utility to backfill old data.")

    # required arguments
    parser.add_argument('-t', "--type", nargs="?", required=True, help="Event type to listen for")
    parser.add_argument('-s', '--start', nargs="?", type=int, required=True, help="Starting unixtime")
    parser.add_argument('-e', '--end', nargs="?", type=int, required=True, help="Ending unixtime")

    parser.add_argument("-r", "--traceroutes", action="store_true", default=False,
                        help="Whether to trigger new traceroutes after backfill")
    parser.add_argument("-f", "--find-gaps", action="store_true", default=False,
                        help="Fill gaps")

    parser.add_argument('-m', '--last-modified-before', nargs="?", type=int, required=False,
                        help="Last modified time before")
    parser.add_argument('-M', '--last-modified-after', nargs="?", type=int, required=False,
                        help="Last modified time after")
    parser.add_argument('-i', '--inserted-before', nargs="?", type=int, required=False,
                        help="Inserted time before")
    parser.add_argument('-I', '--inserted-after', nargs="?", type=int, required=False,
                        help="Inserted time after")

    parser.add_argument('-c', '--cutoff', nargs="?", type=int, required=False,
                        help="Cutoff unixtime for fill gaps, only find events updated before cutoff time")
    parser.add_argument('-p', '--processes', nargs="?", type=int, required=False,
                        default=1,
                        help="Number of processes to divide the time range and run, specify 0 to use all available cores")

    parser.add_argument('-q', '--query-size', nargs="?", type=int, required=False,
                        default=20, help="Number of items to return for each ES query")
    parser.add_argument('-T', '--scroll-timeout', nargs="?", type=str, required=False,
                        default="10m", help="The amount of time a query scroll is valid for")

    parser.add_argument("--missing-inference", action="store_true", default=False,
                        help="Only process events with missing inferences")
    parser.add_argument("--reinference", action="store_true", default=False,
                        help="Only reinference events, skipped tagging")

    # processing finished events
    parser.add_argument("-E", "--enable-finisher", action="store_true", default=False,
                        help="Enable finisher during backfill (only use it in single-threaded execution)")
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Whether to enable debug mode, events will be committed to -test- indices")
    parser.add_argument("-S", "--elastic", action="store_true", default=False,
                        help="Retagging events already on ElasticSearch")
    parser.add_argument("-D", "--datadir", default="/data/bgp/historical",
                        help="Base directory to search for historical data to be reprocessed (ignored if -S is set)")
    parser.add_argument("-P", "--pfxorigindir", default="/data/bgp/historical",
                        help="Base directory to search for pfx2origin data to retag the data with")
    parser.add_argument("-R", "--rpkidir", default="/data/bgp/rpki",
                        help="Base directory to search for RPKI data to retag the data with")

    opts = parser.parse_args()

    # set logging level to be INFO, ElasticSearch operations will be logged to output
    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                        # filename=LOG_FILENAME,
                        level=logging.INFO)

    if opts.find_gaps:
        find_unretagged_timerange(opts.type, opts.start, opts.end, opts.cutoff)  # 1596240000 2020-08-01T00:00:00+0000
        return

    processes = opts.processes
    if processes <= 0:
        processes = os.cpu_count()

    # if processes == 1:
    #     process(opts.type, opts.traceroutes, opts.start, opts.end, opts.enable_finisher, opts.debug, opts.elastic)
    #     return

    if opts.elastic:
        # Retagging events already on ElasticSearch
        reinference = opts.reinference or opts.missing_inference
        QUERY_SIZE = opts.query_size
        es_conn = ElasticConn()
        tasks = multiprocessing.JoinableQueue(maxsize=QUERY_SIZE * 2)
        consumers = [Consumer(opts.type, tasks, reinference, opts.debug) for _ in range(processes)]
        for c in consumers:
            c.start()
        query = query_in_range(opts.start, opts.end,
                               inserted_before=opts.inserted_before,
                               inserted_after=opts.inserted_after,
                               modified_before=opts.last_modified_before,
                               modified_after=opts.last_modified_after,
                               size=QUERY_SIZE)
        if opts.missing_inference:
            query["query"]["bool"]["must_not"].append(
                {
                    "exists": {
                        "field": "summary.inference_result.primary_inference"
                    }
                }
            )
        print(json.dumps(query, indent=4))
        index_pattern = ElasticConn.get_index_name(event_type=opts.type)
        for event in es_conn.search_generator(index=index_pattern, query=query, timeout=opts.scroll_timeout):
            if event is None:
                continue
            tasks.put(event)
            logging.info("put event {} into queue. about {} in queue".format(event.event_id, tasks.qsize()))

        logging.info("adding poison pills to end tasks")
        for i in range(processes):
            # poison pill
            tasks.put(None)

        tasks.join()
        return

    step = int((opts.end - opts.start) / processes)
    cur_ts = opts.start
    args = []
    while cur_ts < opts.end:
        cur_end = cur_ts + step
        args.append((opts.type, opts.traceroutes, cur_ts, cur_end, opts.enable_finisher, opts.debug, opts.elastic, opts.datadir, opts.pfxorigindir, opts.rpkidir))
        cur_ts += step
    print(args)

    with mp.Pool(processes=processes) as pool:
        pool.starmap(process, args)


if __name__ == "__main__":
    main()
