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
import signal
import sys
import time

import grip.common
from grip.events.event import Event
from grip.inference import InferenceEngine
from grip.utils.data.asrank import AsRankUtils
from grip.utils.data.elastic import ElasticConn
from grip.utils.data.hegemony import HegemonyUtils
from grip.utils.kafka import KafkaHelper
from grip.utils.messages import EventOnElasticMsg

DEFAULT_TOPIC_OFFSET = "earliest"

CH_PROC_TIME_TMPL = "bgp.hijacks.meta.inference.%s.processing_time"
CH_RT_DELAY_TMPL = "bgp.hijacks.meta.inference.%s.realtime_delay"
CH_SUSPICIOUS_HIGH_TMPL = "bgp.hijacks.events.inference.%s.suspicion.suspicion_high"
CH_SUSPICIOUS_GREY_TMPL = "bgp.hijacks.events.inference.%s.suspicion.suspicion_grey"
CH_SUSPICIOUS_LOW_TMPL = "bgp.hijacks.events.inference.%s.suspicion.suspicion_low"

KAFKA_POOLING_INTERVAL = 5
DEBUG = False


class InferenceCollector:

    def __init__(self, event_type=None, debug=False):
        self.event_type = event_type
        self.DEBUG = debug

        # ElasticSearch
        self.es_conn = ElasticConn()
        self.kafka_helper = KafkaHelper()
        self.inference_engine = InferenceEngine()

        # External data sources
        self.hegemony = HegemonyUtils()
        self.asrank = AsRankUtils()

    def _init_kafka_consumer(self, event_type):
        """
        Initilaize kafka consumer to listen to messages from the components before inference engine. The following
        two component will pass message to inference engine:
        - tagger: after tagging, the tagger will commit the message to ES, and send a message here to do inference
        - collector: after the active probing results are collected, the active probing collector will send a message

        If on debug mode (indicated by self.DEBUG), the consumer listens to a dedicated debug topic instead.

        :param event_type: event type
        :return:
        """
        # NOTE: we listen to two topics at the inference phase, one after tagging and one after active probing
        consumer_topics = [
            grip.common.get_kafka_topic("tagger", event_type, self.DEBUG),
            grip.common.get_kafka_topic("driver", event_type, self.DEBUG),
            grip.common.get_kafka_topic("collector", event_type, self.DEBUG),
        ]
        consumer_group = grip.common.get_kafka_topic("inference", event_type, self.DEBUG)
        self.kafka_helper.init_consumer(topics=consumer_topics, group_id=consumer_group, offset="earliest")

    def _update_external_data(self, event, to_query_asrank, to_query_hegemony):
        """
        Query data data sources and put the data into the event object
        :param event:
        :param to_query_asrank:
        :param to_query_hegemony:
        :return:
        """
        asns = event.summary.ases
        if to_query_asrank:
            # filter only asns that needed query:
            # 1. we don't have any info for the asn in event.asinfo
            # 2. we have some info for the asn, but not AS rank
            to_query_asns = [asn for asn in asns if asn not in event.asinfo or "asrank" not in event.asinfo[asn]]
            if to_query_asns:
                asrank_res = self.asrank.get_asrank_for_asns(to_query_asns)
                for asn in asrank_res:
                    event.add_to_asinfo(asn, "asrank", asrank_res[asn], override=True)
        if to_query_hegemony:
            # see comments for asrank section above.
            to_query_asns = [asn for asn in asns if asn not in event.asinfo or "hegemony" not in event.asinfo[asn]]
            if to_query_asns:
                hegemony_res = self.hegemony.query_hegemony(
                    subgraph_asn_lst=["0"], asn_lst=to_query_asns, timestamp=event.view_ts)["0"]
                for asn in hegemony_res:
                    event.add_to_asinfo(asn, "hegemony", hegemony_res[asn], override=True)

    def infer_event(self, event, to_query_asrank=True, to_query_hegemony=False):
        """
        Given an Event object, perform inference on the event.

        :param event: an Event object
        :param to_query_asrank: whether to query for asrank data
        :param to_query_hegemony: whether to query for hegemony data
        :return:
        """
        assert isinstance(event, Event)
        start_time = time.time()

        # call inference engine to conduct inference and update the event object
        self.inference_engine.infer_on_event(event)

        # extract data info, only querying it once
        self._update_external_data(event, to_query_hegemony=to_query_hegemony, to_query_asrank=to_query_asrank)

        # log inference processing time
        event.event_metrics.proc_time_inference = time.time() - start_time

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

        assert self.event_type is not None
        self._init_kafka_consumer(self.event_type)

        while True:
            if shutdown["count"] > 0:
                logging.info("Shutting down")
                break

            # quickly polling all pending messages from kafka before processing results
            msg = self.kafka_helper.poll(KAFKA_POOLING_INTERVAL)
            if msg is None or msg.error():
                continue
            msg_str = msg.value().decode("utf-8")
            event_ready_msg = EventOnElasticMsg.from_str(msg_str)
            logging.debug("received new event ready message: {}".format(msg_str))

            if "v3" not in event_ready_msg.es_index:
                logging.info("skipping inference for wrong index: {}".format(event_ready_msg.es_index))
                self.kafka_helper.commit_offset()
                continue

            # retrieve event object from ElasticSearch
            event = self.es_conn.get_event_by_id(index=event_ready_msg.es_index, event_id=event_ready_msg.es_id)
            if event is None:
                logging.warning(
                    "Failed to retrieve event: {}/{}".format(event_ready_msg.es_index, event_ready_msg.es_id))
                continue

            # conduct inference, the event object will be updated by the function
            self.infer_event(event)
            self.es_conn.index_event(event, index=event_ready_msg.es_index, update=True)  # commit updated event back to ES

            # commit kafka offset, move kafka server pointer forward by one message.
            # NOTE: it is very important to commit offset when finished processing events.
            #       depending on the situation, we may consider reduce commit frequency.
            self.kafka_helper.commit_offset()


def main():
    parser = argparse.ArgumentParser(
        description="Utility to listen for new events and trigger active measurements.")

    parser.add_argument('-t', "--type", nargs="?", required=True,
                        help="Event type to listen for")
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Whether to enable debug mode")

    opts = parser.parse_args()

    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s", level=logging.INFO)
    # use the following line to reduce log messages produced by elasticsearch
    # logging.getLogger('elasticsearch').setLevel(logging.WARN)

    InferenceCollector(event_type=opts.type, debug=opts.debug).listen()


if __name__ == "__main__":
    main()
