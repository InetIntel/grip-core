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
The finisher processes all new and finished events and mark corresponding finished events as finished.
"""

import logging

from grip.common import KAFKA_TOPIC_TEMPLATE
from grip.events.event import Event
from grip.events.pfxevent import PfxEvent
from grip.utils.data.elastic import ElasticConn
from grip.utils.data.elastic_queries import query_unfinished_events
from grip.utils.kafka import KafkaHelper
from grip.utils.messages import EventOnElasticMsg
from grip.utils.transition import TransitionLocator


def extract_pfx_event_feature(pfx_event, event_type=None):
    if isinstance(pfx_event, PfxEvent):
        feature = "%s-%s" % (pfx_event.event_type, pfx_event.details.get_prefix_fingerprint())
    elif isinstance(pfx_event, dict):
        assert event_type is not None
        if event_type == "moas" or event_type == "edges":
            prefixes = [pfx_event["details"]["prefix"]]
        else:
            # submoas and defcon
            prefixes = [pfx_event["details"]["super_pfx"], pfx_event["details"]["sub_pfx"]]
        prefixes = [f.replace("/", "-") for f in prefixes]
        feature = "{}-{}".format(event_type, "_".join(prefixes))
    else:
        raise ValueError("unknown pfx event data type")
    return feature


class Finisher:

    def __init__(self, event_type, load_unfinished=True, index_pattern=None,
                debug=False, pfx_datadir="/data/bgp/historical"):
        assert (event_type in ["moas", "submoas", "defcon", "edges"])
        self.event_type = event_type
        self.esconn = ElasticConn()

        self.debug = debug
        self.unfinished_events = {}
        self.unfinished_pfx_events = {}

        if index_pattern:
            self.index_name_pattern = index_pattern
        else:
            self.index_name_pattern = self.esconn.get_index_name(event_type,
                    debug=self.debug)

        if load_unfinished:
            self.load_unfinished_events(event_type)

        self.transition_locator = TransitionLocator(event_type, pfx_datadir)

        self.unchecked_transition_events = {}
        kafka_template = KAFKA_TOPIC_TEMPLATE
        self.kafka_producer_topic = kafka_template % ("tagger", event_type)
        self.kafka_producer = KafkaHelper()
        self.kafka_producer.init_producer(topic=self.kafka_producer_topic)

    @staticmethod
    def _update_finished_ts(event: Event, event_finished, pfx_feature_to_finished_ts, time_ts):
        """
        Mark an event and prefix events finished.
        :param event:
        :param event_finished:
        :param pfx_feature_to_finished_ts: dictionary that maps pfx events feature to finished ts
        :param time_ts:
        """
        # if the entire event is finished, set event's `finished_ts` to current time_ts
        if event_finished:
            event.finished_ts = time_ts

        # update pfx_event's finished_ts field
        for pfx_event in event.pfx_events:
            pfx_event.finished_ts = pfx_feature_to_finished_ts.get(
                extract_pfx_event_feature(pfx_event), None)

    def _check_transition(self, event: Event):
        """
        check if an event is a transition event, i.e. prefix origins changes from A -> AB -> B.
        core logic:
        1. check if the MOAS event contains two origins
        2. if at the finished time the prefix origin is the newcomer, i.e. B, then the event is a transition event

        If the data is not available, throw ValueError out to be captured upstream.
        :param event:
        :return:
        """

        if not event.finished_ts or event.event_type != "moas":
            return

        assert (isinstance(event.finished_ts, int))
        assert (isinstance(event.view_ts, int))

        is_transition = self.transition_locator.check_event_transition(event)
        # update event on elasticsearch and send a message to kafka
        self.transition_locator.update_event(event, is_transition)
        return is_transition

    def load_unfinished_events(self, event_type, limit=10000, start_ts=None, end_ts=None):
        logging.info("FINISHER: loading unfinished events")
        source_fields = ["id", "view_ts", "pfx_events.finished_ts", "pfx_events.details.prefix",
                         "pfx_events.details.super_pfx", "pfx_events.details.sub_pfx",
                         "pfx_events.details.as1", "pfx_events.details.as2"]
        query = query_unfinished_events(source_fields=source_fields, start_ts=start_ts, end_ts=end_ts)
        for item in self.esconn.search_generator(index=self.index_name_pattern,
                                                 query=query, raw_json=True, limit=limit):
            # basic info
            event_id = item["id"]
            view_ts = item["view_ts"]

            # prefix event info
            features = set()
            for pfx_event in item["pfx_events"]:
                if pfx_event["finished_ts"] is not None:
                    # already finished
                    continue

                feature = extract_pfx_event_feature(pfx_event, event_type)
                features.add(feature)
                if feature not in self.unfinished_pfx_events:
                    self.unfinished_pfx_events[feature] = set()
                # save the event id to the unfinished pfx events list
                # the same prefix event could appear in multiple events
                self.unfinished_pfx_events[feature].add((view_ts, event_id))

            self.unfinished_events[event_id] = {
                "index": self.esconn.infer_index_name_by_id(event_id,
                        debug=self.debug),
                "unfinished": list(features),
                "finished": {}
            }

    def process_finished_event(self, event):
        """
        Process finished events, marking event's finished_ts and update the duration of the events.

        :param event:
        :return:
        """
        assert (isinstance(event, Event))
        assert (event.position == "FINISHED")

        view_ts = event.view_ts

        updated_events = set()
        # extract pfx_event features
        features = set([extract_pfx_event_feature(pfx_event) for pfx_event in event.pfx_events])
        for pfx_feature in features:
            # new finished pfx event that was not logged previously, ignore for now
            if pfx_feature not in self.unfinished_pfx_events:
                continue

            # if the current prefix event is known to be unfinished, then finish it.
            # the set self.unfinished_pfx_events is populated during the process of new events.

            # find all unfinished events before the current timestamp that contains the current prefix event
            all_event_ts_id = self.unfinished_pfx_events.pop(pfx_feature)
            try:
                event_ts_id_set = {(ts, event_id) for (ts, event_id) in all_event_ts_id if ts <= view_ts}
            except ValueError as e:
                logging.error("%s" % all_event_ts_id)
                raise e

            if len(all_event_ts_id) != len(event_ts_id_set):
                # if there are events that contains the prefix event but are from the future,
                # put them back to the unfinished_pfx_events dict
                self.unfinished_pfx_events[pfx_feature] = all_event_ts_id - event_ts_id_set

            for (_, event_id) in reversed(sorted(event_ts_id_set)):
                # sorted by timestamp, from newest to oldest
                if event_id not in self.unfinished_events:
                    # unfinished_events has not record of this event
                    # it is possible that for the first few
                    continue
                event_data = self.unfinished_events[event_id]
                if pfx_feature in event_data["unfinished"]:
                    # found a unfinished
                    event_data["unfinished"].remove(pfx_feature)
                    event_data["finished"][pfx_feature] = view_ts
                    updated_events.add(event_id)
                    # we only update the most recent match

        updated = False
        for event_id in updated_events:
            event_finished = False
            # for all the updated events, we should push it to ElasticSearch
            if event_id not in self.unfinished_events:
                continue
            event_data = self.unfinished_events[event_id]
            if len(event_data["unfinished"]) == 0:
                # no more unfinished pfx events for this event
                # this event is finished
                self.unfinished_events.pop(event_id)
                event_finished = True

            res_event = self.esconn.get_event_by_id(event_id=event_id,
                        debug=self.debug)
            if res_event is None:
                logging.warning("cannot retrieve {} for marking it as finished", event_id)
                continue

            # mark the event's finished prefix events as finished
            self._update_finished_ts(res_event, event_finished, event_data["finished"], view_ts)
            is_transition = False
            try:
                is_transition = self._check_transition(res_event)
            except ValueError:
                # The ValueError exception is raised in transion.py:check_event_transition, indicating that pfx-origins
                # data file needed for checking transition is not yet available at the time of the consumer producing
                # this MOAS event.
                # In other words, this happens when MOAS consumer is faster than pfx-origins consumer.
                finished_ts = int(res_event.finished_ts)
                if finished_ts not in self.unchecked_transition_events:
                    self.unchecked_transition_events[finished_ts] = []
                self.unchecked_transition_events[finished_ts].append(res_event)

            # update event on elasticsearch.
            self.esconn.index_event(event=res_event, update=True,
                        debug=self.debug)
            if is_transition:
                # only send signal for reinference if it is a transition event (MOAS only)
                kafka_msg = EventOnElasticMsg(
                    sender="tagger",
                    es_index=self.esconn.infer_index_name_by_id(event.event_id,
                                debug=self.debug),
                    es_id=event.event_id,
                    tr_worthy=False,  # do not trigger active probing actions
                )
                self.kafka_producer.produce(kafka_msg.to_str(), flush=True)
            updated = True

        return updated

    def recheck_transition_events(self):
        """
        Recheck previously unchecked finished events to determine if they're transition events.
        The unchecked events are caused by pfx-origins data not yet available when checking for event's origins.
        :return:
        """
        finished_views = []
        for view_ts in sorted(self.unchecked_transition_events.keys()):
            try:
                for event in self.unchecked_transition_events[view_ts]:
                    is_transition = self._check_transition(event=event)
                    if is_transition:
                        self.esconn.index_event(event=event, update=True,
                                debug=self.debug)
                        # only send signal for reinference if it is a transition event (MOAS only)
                        kafka_msg = EventOnElasticMsg(
                            sender="tagger",
                            es_index=self.esconn.infer_index_name_by_id(event.event_id, debug=self.debug),
                            es_id=event.event_id,
                            tr_worthy=False,  # do not trigger active probing actions
                        )
                        self.kafka_producer.produce(kafka_msg.to_str(), flush=True)
            except ValueError:
                break
            finished_views.append(view_ts)

        for view_ts in finished_views:
            # remove all views that has finished processing
            self.unchecked_transition_events.pop(view_ts)

    def process_new_event(self, event, index_name=None):
        assert (isinstance(event, Event))
        if index_name is None:
            index_name = self.esconn.infer_index_name_by_id(event.event_id,
                        debug=self.debug)
        features = set()
        for pfx_event in event.pfx_events:
            assert (isinstance(pfx_event, PfxEvent))
            feature = extract_pfx_event_feature(pfx_event)
            features.add(feature)
            if feature not in self.unfinished_pfx_events:
                self.unfinished_pfx_events[feature] = set()
            # save the event id to the unfinished pfx events list
            # the same prefix event could appear in multiple events
            self.unfinished_pfx_events[feature].add((event.view_ts, event.event_id))

        # each event can only have a list of unfinished pfx events represented by their features
        self.unfinished_events[event.event_id] = {
            "index": index_name,
            "unfinished": list(features),
            "finished": {}
        }
