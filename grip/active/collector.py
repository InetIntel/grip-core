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

import logging
import signal
import sys
import time
from enum import Enum

from ripe.atlas.cousteau import AtlasResultsRequest

from grip.active.as_traceroute import AsTracerouteDriver
from grip.active.ripe_atlas.ripe_atlas_msm import AtlasMeasurement
from grip.active.ripe_atlas.ripe_atlas_utils import extract_atlas_response, get_traceroute_status_map
from grip.common import get_kafka_topic
from grip.events.event import Event
from grip.events.pfxevent import PfxEvent
from grip.redis import Pfx2AsHistorical
from grip.utils.data.elastic import ElasticConn
from grip.utils.kafka import KafkaHelper
from grip.utils.messages import MeasurementsRequestedMsg, EventOnElasticMsg

REQUEST_RETRY_INTERVAL = 30  # number of seconds to wait before asking again for results
KAFKA_POOLING_INTERVAL = 5


class TRACEROUTE_STATUS(Enum):
    Ongoing = 1
    Finished = 2
    Failed = 3
    RetrievalError = 4


class ActiveProbingCollector:

    def __init__(self, event_type, debug=False):
        self.DEBUG = debug
        producer_topic = get_kafka_topic("collector", event_type, debug)  # produce as driver
        consumer_topic = get_kafka_topic("driver", event_type, debug)  # consumer from tagger
        consumer_group = producer_topic
        self.OUTPUT_CONTAINER_TMPL = "bgp-hijacks-%s-traceroutes"
        if debug:
            self.OUTPUT_CONTAINER_TMPL = "bgp-hijacks-%s-traceroutes-debug"

        self.event_type = event_type

        # prefix-to-as mapping
        # self._pfx_origin_db = Pfx2AsNewcomer()
        self._pfx_origin_db = Pfx2AsHistorical()

        # initialize kafka helper
        self.kafka_helper = KafkaHelper()
        self.kafka_helper.init_producer(topic=producer_topic)
        self.kafka_helper.init_consumer(topics=[consumer_topic], group_id=consumer_group, offset="earliest")

        # ElasticSearch
        self.es_conn = ElasticConn()

    @staticmethod
    def _retrieve_results_map(msm_ids):
        """
        retrieve traceroute results by measurement IDs
        :param msm_ids:
        :return: map of traceroute results, key: measurement id, value: traceroute result json
        """

        results_map = {}
        if not msm_ids:
            return results_map
        status_map = get_traceroute_status_map(msm_ids)
        if not status_map:
            return {}

        for msm_id in msm_ids:
            """
            status (integer): 0: Specified, 1: Scheduled, 2: Ongoing, 4: Stopped, 5: Forced to stop, 6: No suitable probes, 7: Failed, 8: Archived
            """

            if msm_id not in status_map:
                # msm_id should be in status_map, reaching here indicates something went wrong during status checking
                # NOTE: yield does not mean continue. when the downstream process finishes, code will go executing the next lines.
                yield msm_id, (TRACEROUTE_STATUS.RetrievalError, [])
                continue

            status = status_map[msm_id]
            if not status:
                yield msm_id, (TRACEROUTE_STATUS.Failed, [])
                continue
            if status["id"] < 4:
                yield msm_id, (TRACEROUTE_STATUS.Ongoing, [])
                continue
            if status["id"] > 5:
                yield msm_id, (TRACEROUTE_STATUS.Failed, [])
                continue

            # reaching here means the status is either 4: Stopped, or 5: Forced to stop
            # meaning we can extract the results now
            is_success, responses = AtlasResultsRequest(msm_id=msm_id).create()
            if is_success:
                yield msm_id, (TRACEROUTE_STATUS.Finished, responses)
                continue

            yield msm_id, (TRACEROUTE_STATUS.Failed, [])

    def _process_finished_measurements(self, measurements):
        event_id_set = set()
        msm_map = {}

        for msm in measurements:
            assert (isinstance(msm, AtlasMeasurement))
            event_id_set.add(msm.event_id)
            msm_map[msm.msm_id] = msm

        for event_id in event_id_set:
            event = self.es_conn.get_event_by_id(event_id=event_id)
            if event is None:
                logging.warning("cannot find event {}".format(event_id))
                continue
            assert (isinstance(event, Event))

            # update prefix event traceroutes
            for pfx_event in event.pfx_events:
                assert (isinstance(pfx_event, PfxEvent))
                new_msms = []
                for msm in pfx_event.traceroutes["msms"]:
                    if msm.msm_id in msm_map:
                        # need to replace this measurement
                        new_msms.append(msm_map[msm.msm_id])
                    else:
                        new_msms.append(msm)
                pfx_event.traceroutes["msms"] = new_msms

            # recommit event to ElasticSearch
            self.es_conn.index_event(event=event, update=True)

            # notify downstream (i.e. inference engine) that new updated events are ready to be retrieved
            msg = EventOnElasticMsg(
                sender="ripe-collector",
                es_index=self.es_conn.infer_index_name_by_id(event.event_id),
                es_id=event_id,
                tr_worthy=event.summary.tr_worthy)
            self.kafka_helper.produce(value_str=msg.to_str(), flush=True)

    def listen(self):
        """listen for traceroute request IDs from driver and retrieve results"""

        shutdown = {"count": 0}

        def _stop_handler(_signo, _stack_frame):
            logging.info("Caught signal, shutting down at next opportunity")
            shutdown["count"] += 1
            if shutdown["count"] > 3:
                logging.warning("Caught %d signals, shutting down NOW", shutdown["count"])
                sys.exit(0)

        signal.signal(signal.SIGTERM, _stop_handler)
        signal.signal(signal.SIGINT, _stop_handler)

        # measurement jobs
        msms_map = {}
        proc_time_map = {}

        prev_measurements = []

        as_traceroute_driver = AsTracerouteDriver()
        while True:

            if shutdown["count"] > 0:
                logging.info("Shutting down")
                break

            msg = self.kafka_helper.poll(KAFKA_POOLING_INTERVAL)

            # quickly polling all pending messages from kafka before processing results
            if msg is not None and not msg.error():
                msms_msg = MeasurementsRequestedMsg.from_str(msg.value().decode("utf-8"))

                view_ts = msms_msg.view_ts

                if view_ts not in proc_time_map:
                    # record the first time receiving events from the timestamp
                    proc_time_map[view_ts] = time.time()

                # collect measurement from event
                for msm in msms_msg.measurements:
                    assert (isinstance(msm, AtlasMeasurement))
                    if msm.msm_id > 0:
                        # only save succeeded measurements
                        msms_map[int(msm.msm_id)] = msm
                continue  # continue to poll next kafka message

            # all messages from Kafka have been registered, continue to processing the measurements
            # check current measurements to see if new results have come
            if len(msms_map.keys()) != len(prev_measurements):
                # only log changes of pending measurement results
                prev_measurements = msms_map.keys()
                logging.info("ongoing measurements updated (total {} stored in cache): {}".format(len(msms_map.keys()), msms_map.keys()))

            finished_msms = []
            for msm_id, (status, responses) in self._retrieve_results_map(list(msms_map)):
                if shutdown["count"] > 0:
                    logging.info("Shutting down")
                    break
                if msm_id not in msms_map:
                    logging.warn("retrieved measurement {} but msms_map does not have it".format(msm_id))
                    continue
                msm = msms_map.pop(msm_id)
                assert (isinstance(msm, AtlasMeasurement))
                if status == TRACEROUTE_STATUS.RetrievalError:
                    # failed to retrieve status from Atlas
                    logging.error("cannot retrieve result for measurement %d, discard now", msm_id)
                    continue
                elif status == TRACEROUTE_STATUS.Ongoing:
                    # on-going measurement, put popped measurement object back into map
                    msms_map[msm_id] = msm
                    continue
                elif status == TRACEROUTE_STATUS.Failed:
                    # measurement failed
                    logging.info("measurement %d failed (https://atlas.ripe.net/api/v2/measurements/%s/): %s",
                                 msm_id, msm_id, responses)
                    logging.info("%s", msm.as_str())
                    msm.results = []
                    msm.request_error = responses
                    finished_msms.append(msm)
                    continue
                elif status == TRACEROUTE_STATUS.Finished:
                    logging.info("processing results for measurement {} for event {}".format(msm_id, msm.event_id))
                    view_ts = msm.event_id.split("-")[1]
                    traceroute_results = extract_atlas_response(pfx_origin_db=self._pfx_origin_db, responses=responses,
                                                                target_pfx=msm.target_pfx)
                    as_traceroute_driver.fill_as_traceroute_results(traceroute_results=traceroute_results, view_ts=view_ts)
                    msm.results = traceroute_results
                    finished_msms.append(msm)

            if finished_msms:
                logging.info("updating events for {} finished measurements".format(len(finished_msms)))
                self._process_finished_measurements(finished_msms)
                logging.info("updating kafka offset")
                self.kafka_helper.commit_offset()
