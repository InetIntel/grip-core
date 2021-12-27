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

import json
import logging
import signal
import sys
import time

from future.utils import iteritems

import grip.common
import grip.coodinator.announce
from grip.active.ripe_atlas import target_ip_generator
from grip.active.ripe_atlas.ripe_atlas_probe import ProbeSelector
from grip.active.ripe_atlas.ripe_atlas_utils import RipeAtlasUtils
from grip.common import *
from grip.events.event import Event
from grip.events.pfxevent import PfxEvent
from grip.utils.data.elastic import ElasticConn
from grip.utils.general import to_dict
from grip.utils.kafka import KafkaHelper
from grip.utils.messages import EventOnElasticMsg, MeasurementsRequestedMsg

TR_DISABLED = {
    "moas": False,
    "submoas": False,
    "edges": False,
    "defcon": False,
}


class ActiveProbingDriver:
    """active probing driver"""

    def __init__(self, event_type, key=None, debug=False):
        self.DEBUG = debug
        producer_topic = get_kafka_topic("driver", event_type, debug)  # produce as driver
        consumer_topic = get_kafka_topic("tagger", event_type, debug)  # consumer from tagger
        consumer_group = get_kafka_topic("driver", event_type, debug)  # group set to be the same as producer topic

        if key is None:
            key = os.environ.get('ATLASKEY')
            assert key is not None

        self.event_type = event_type
        self.traceroute = RipeAtlasUtils(key=key, num_probes=ACTIVE_MAX_PROBES_PER_TARGET)
        self.probe_selector = ProbeSelector(event_type)
        self.tr_event_count = {}  # count of events requested traceroutes per bin

        # kafka-related initialization
        self.kafka_helper = KafkaHelper()
        self.kafka_helper.init_producer(topic=producer_topic)
        self.kafka_helper.init_consumer(topics=[consumer_topic], group_id=consumer_group, offset="earliest")

        # ElasticSearch
        self.es_conn = ElasticConn()

    def request_measurements(self, event):
        """
        Send RIPE Atlas traceroute requests for a given event.
        :param event:
        :return:
        """
        assert isinstance(event, Event)
        # local variables
        all_requested_jobs = []
        all_failed_jobs = []
        all_succeeded_jobs = []

        # extract traceroute worthy prefix events
        tr_worthy_pfx_events = [pfx_event for pfx_event in event.pfx_events if
                                pfx_event.traceroutes["worthy"]]
        event.tr_metrics.tr_worthy_pfx_event_cnt += len(tr_worthy_pfx_events)
        event_ases = list(event.summary.ases)
        event.tr_metrics.tr_worthy = True
        event.tr_metrics.total_event_as_cnt = len(event_ases)

        if event.view_ts not in self.tr_event_count:
            self.tr_event_count = {event.view_ts: 0}  # clear counter for a new bin

        # check if we've already executed traceroute measurements for max number of events
        # or if it's explicitly disabled
        if TR_DISABLED[self.event_type]:
            event.tr_metrics.tr_skipped = True
            event.tr_metrics.tr_skip_reason = "traceroute disabled for %s" % self.event_type
            logging.info("skipping traceroutes due to explicit disabling")
            return []
        if self.tr_event_count[event.view_ts] >= ACTIVE_MAX_EVENTS_PER_BIN:
            event.tr_metrics.tr_skipped = True
            event.tr_metrics.tr_skip_reason = "reached self-imposed rate limit of %d per five minutes for %s" \
                                              % (ACTIVE_MAX_EVENTS_PER_BIN, self.event_type)
            logging.info("skipping traceroutes due to rate limiting")
            return []

        # increase the counter and continue requesting traceroute
        self.tr_event_count[event.view_ts] += 1

        # get vantage points for the event
        # note that all prefix events should share the same set of vantage points
        asn_probes_mapping = {}
        logging.debug("start getting probes for event ASes %s", event_ases)
        self.probe_selector.update_asrank(event.view_ts)
        succeeded = 0
        for asn in event_ases:
            asn = str(asn)
            # Pick the given number of adjacent probes according to the given ASN
            probes = self.probe_selector.pick_adjacent_probes(asn, ACTIVE_MAX_PROBES_PER_TARGET)
            logging.info("found {} probes for asn {}".format(len(probes), asn))
            if probes:
                succeeded += 1
                asn_probes_mapping[asn] = probes
            if succeeded == ACTIVE_MAX_EVENT_ASES:
                break
        all_vps = set()
        for _, probes in iteritems(asn_probes_mapping):
            all_vps.update([probe.asn for probe in probes])

        event.tr_metrics.selected_vp_cnt = len(all_vps)
        event.tr_metrics.selected_unique_vp_cnt = len(set(all_vps))
        event.tr_metrics.selected_event_as_cnt = len(asn_probes_mapping)

        # now go through the pfx events and pick some to traceroute
        # this also generates the IP addresses that we will traceroute
        probe_pfx_ip_map = self._select_target_ip(event)
        logging.info("start sending atlas requests")
        for pfx_event in tr_worthy_pfx_events[:ACTIVE_MAX_PFX_EVENTS]:
            assert isinstance(pfx_event, PfxEvent)
            # get probe IP
            target_prefix = pfx_event.details.get_prefix_of_interest()

            if target_prefix is None or target_prefix not in probe_pfx_ip_map:
                event.tr_metrics.tr_skipped = True
                event.tr_metrics.tr_skip_reason = "cannot find traceroute target prefix for %s" % target_prefix
                logging.info("skipping pfx event due to lack of target prefix")
                continue

            target_ip = probe_pfx_ip_map[target_prefix]
            event.tr_metrics.selected_pfx_event_cnt += 1

            jobs_succeeded, jobs_failed = self.traceroute.create_request(
                target_ip=target_ip,
                target_pfx=target_prefix,
                asn_probes_mapping=asn_probes_mapping,
                event_id=event.event_id
            )

            all_requested_jobs.extend(jobs_succeeded + jobs_failed)
            all_succeeded_jobs.extend(jobs_succeeded)
            all_failed_jobs.extend(jobs_failed)

            # add succeeded requests to pfx_event
            pfx_event.traceroutes["msms"].extend(jobs_succeeded+jobs_failed)

        if not all_requested_jobs:
            event.tr_metrics.tr_skipped = True
            event.tr_metrics.tr_skip_reason = "failed to create requests for  {} tr_worthy prefix events"\
                .format(len(tr_worthy_pfx_events[:ACTIVE_MAX_PFX_EVENTS]))
        else:
            if len(all_requested_jobs) == len(all_failed_jobs):
                event.tr_metrics.tr_skipped = True
                event.tr_metrics.tr_skip_reason = "all traceroute requests (%d) failed" % len(all_requested_jobs)

        event.tr_metrics.tr_request_cnt += sum([len(job.probe_ids) for job in all_requested_jobs])
        event.tr_metrics.tr_request_failure_cnt += sum([len(job.probe_ids) for job in all_failed_jobs])

        return all_succeeded_jobs

    @staticmethod
    def _select_target_ip(event):
        # create a pfx2probe-ip dataset
        ip_gen = grip.active.ripe_atlas.target_ip_generator.TargetIpGenerator()
        for pfx_event in event.pfx_events:
            assert isinstance(pfx_event, PfxEvent)
            try:
                ip_gen.add_pfx(pfx_event.details.get_prefix_of_interest())
            except ValueError as e:
                logging.error("cannot find probe IP for prefix: {}".format(pfx_event.details.get_prefix_of_interest()))
                raise e

        # get a mapping from prefixes to probe IP
        pfx_ip_map = ip_gen.get_probe_pfx_ip_map()

        return pfx_ip_map

    def process_event(self, event):
        """
        Given a Event object, process and request for active measurements.

        :param event:
        :return:
        """
        # this event must be traceroute worthy
        assert event.summary.tr_worthy

        start_time = time.time()

        # request measurements
        succeeeded_msm_requests = self.request_measurements(event)
        event.event_metrics.proc_time_driver = time.time() - start_time
        logging.info("finished processing event %s: %s", event.event_id, json.dumps(to_dict(event.tr_metrics)))

        # update the event on ElasticSearch
        self.es_conn.index_event(event=event, debug=self.DEBUG, update=True)

        if len(succeeeded_msm_requests) > 0:
            # send a message to collector to let it know that some event's traceroute has been requested
            msg = MeasurementsRequestedMsg.from_event(
                sender="driver",
                event_type=event.event_type,
                view_ts=event.view_ts,
                event_id=event.event_id,
                measurements=succeeeded_msm_requests)
            logging.info("sending MeasurementsRequestedMsg to collector: {}".format(msg.to_str()))
            self.kafka_helper.produce(value_str=msg.to_str(), flush=True)

    def listen(self, limit=float("inf")):
        """
        Listen to EventReadyMsg coming in from Tagger via Kafka. The message contains information on how to retrieve
        event object from ElasticSearch. The active probing driver and collector only updates the event object on
        ElasticSearch if necessary.

        :param limit: maximum number of messages it processes in current execution
        """
        shutdown = {"count": 0}

        def _stop_handler(_signo, _stack_frame):
            logging.info("Caught signal, shutting down at next opportunity")
            shutdown["count"] += 1
            if shutdown["count"] > 3:
                logging.warning("Caught %d signals, shutting down NOW" % shutdown["count"])
                sys.exit(0)

        signal.signal(signal.SIGTERM, _stop_handler)
        signal.signal(signal.SIGINT, _stop_handler)

        msg_count = 0
        while True:
            if shutdown["count"] > 0 or msg_count >= limit:
                # shutdown the service if sigint/sigterm received, or reach msg limit
                logging.info("Shutting down")
                break

            # retrieve message from kafka
            msg = self.kafka_helper.poll(5)
            if msg is None or msg.error():
                self.kafka_helper.commit_offset()
                continue
            msg_count += 1

            # at this point, we have a good EventOnElasticMsg object from the tagger.
            # Example message:
            # 'tagger observatory-test-moas-2019-9-30 moas-1569847800-5602_7713 event_result False'
            event_ready_msg = EventOnElasticMsg.from_str(msg.value().decode("utf-8"))
            if not event_ready_msg.tr_worthy or "v2" not in event_ready_msg.es_index:
                # if the event is not tr_worthy, don't bother doing anything forward
                self.kafka_helper.commit_offset()
                continue

            # now the event is tr_worthy
            # retrieve event from ElasticSearch and parse it into Event object
            event = self.es_conn.get_event_by_id(index=event_ready_msg.es_index, event_id=event_ready_msg.es_id)
            if event is None:
                logging.info("cannot retrieve event from {}".format(event_ready_msg.to_url()))
                self.kafka_helper.commit_offset()
                continue
            # check if the event is too old for conducting traceroutes, threshold is defined in ACTIVE_MAX_TIME_DELTA
            if time.time() - event.view_ts > ACTIVE_MAX_TIME_DELTA:
                # event is too old to worth traceroute
                logging.info("event {} is older than {} seconds before now, skipping traceroute"
                             .format(event.event_id, ACTIVE_MAX_TIME_DELTA))
                self.kafka_helper.commit_offset()
                continue
            self.process_event(event)
            self.kafka_helper.commit_offset()
        # end of while True loop

