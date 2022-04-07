"""
    This script contains a number of code snippets to locate prefix origin transitioning events from all MOAS events.
"""
import argparse
import logging

from grip.common import KAFKA_TOPIC_TEMPLATE
from grip.events.event import Event
from grip.utils.data.elastic import ElasticConn
from grip.utils.data.pfx_origins import load_pfx_file
from grip.utils.kafka import KafkaHelper


# noinspection PyTypeChecker
class TransitionLocator:
    """
    1. get all 5-minute MOAS events
    2. check for each event, if the prefix origins are A -> AB -> B
    3. add special tags to such events
    """

    def __init__(self, event_type, pfx_datadir="/data/bgp/historical"):
        self.es_conn = ElasticConn()
        self.pfx_origins = {
            "time": None,
            "pfx2as": {},
        }
        kafka_template = KAFKA_TOPIC_TEMPLATE
        self.kafka_producer_topic = kafka_template % ("tagger", event_type)
        self.kafka_producer = KafkaHelper()
        self.kafka_producer.init_producer(topic=self.kafka_producer_topic)
        self.pfx_datadir = pfx_datadir

    def _update_pfx_origins(self, timestamp):
        assert (isinstance(timestamp, int))

        ts = timestamp
        if self.pfx_origins["time"] != ts:
            # if the current dataset's timestamp is not what we wanted, we need to reload

            pfx2as = load_pfx_file(self.pfx_datadir, ts)
            if pfx2as is None:
                raise ValueError("data not available yet")
            self.pfx_origins["pfx2as"] = pfx2as
            self.pfx_origins["time"] = ts

    def update_event(self, event, is_transition):
        """
        Update event object after transition, also send kafka message to notify
        the inference engine for re-inference afterwards.

        :param event:
        :param is_transition:
        :return:
        """
        assert (isinstance(event, Event))
        assert (event.event_type == "moas")

        event.debug["transition_checked"] = True

        if is_transition:
            # if the event is a transition event, then all the prefix events
            # within this event is also a transition event
            tags_to_add = ["moas-transition"]
            if event.finished_ts - event.view_ts <= 300:
                tags_to_add.append("moas-potential-convergence")
            else:
                tags_to_add.append("moas-potential-transfer")
            # add tags to all the prefix events in the event
            for pfx_event in event.pfx_events:
                pfx_event.add_tags(tags_to_add)

    def check_transition_by_id(self, event_id, update=False):
        event = self.es_conn.get_event_by_id(event_id)
        if event is None:
            return
        is_transition = self.check_event_transition(event)
        if update:
            self.update_event(event, is_transition)
        return is_transition

    def check_event_transition(self, event):
        """
        Check if an event is caused by ownership transition, i.e. prefixes ownership has A -> AB -> B pattern
        :param event:
        :return: True if transition, and the prefixes ownership in the three views
        """
        assert (isinstance(event, Event))

        # sanity check the event first
        if event.event_type != "moas" \
                or not event.finished_ts \
                or len(event.summary.prefixes) == 0 \
                or len(event.summary.ases) != 2 \
                or len(event.pfx_events[0].details.get_new_origins()) != 1:
            # if event is not moas, event has no prefix (shouldn't happen),
            # or has not finished, it's not a transition
            return False

        data_timestamp = event.finished_ts

        # update
        try:
            self._update_pfx_origins(data_timestamp)
        except TypeError as e:
            logging.error("{}".format(event.as_json()))
            raise e
        except ValueError as e:
            logging.warn("pfx-origins data not yet available for time {}".format(event.finished_ts))
            raise e

        newcomer = str(list(event.pfx_events[0].details.get_new_origins())[0])

        for prefix in event.summary.prefixes:
            try:
                origins = self.pfx_origins['pfx2as'].get(prefix, [])

                if len(origins) != 1 or str(origins[0]) != newcomer:
                    # the newcomer is not the current prefix owner at the event finished time
                    return False
            except KeyError:
                # break if data is not available
                break

        return True

    def locate_events(self, limit=-1, update=False, suspicious_only=False):
        """
        script to locate 5 minute MOAS events
        :return:
        """

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"exists": {
                            "field": "finished_ts"
                        }}
                    ],
                    "must_not": [
                        {
                            "match": {
                                "tags": "moas-transition"
                            }
                        },
                        {
                            "exists": {
                                "field": "debug.transition_checked"
                            }
                        }
                    ]
                }
            },
            "sort": {
                "finished_ts": {
                    "order": "desc"
                }
            }
        }

        if limit > 0:
            query["size"] = limit

        if suspicious_only:
            query["query"]["bool"]["must"].append({
                "range": {
                    "inference.suspicion.suspicion_level": {
                        "gte": 80
                    }
                }
            }
            )

        for event in self.es_conn.search_generator(index="observatory-events-moas-*", query=query, limit=limit):
            assert (isinstance(event, Event))
            try:
                is_transition = self.check_event_transition(event)
                if update:
                    self.update_event(event, is_transition)
                logging.info("{}: {}".format(event.event_id, is_transition))
            except ValueError:
                pass


def main():
    parser = argparse.ArgumentParser(
        description="Utility the check if event(s) is caused by ownership transition")

    parser.add_argument('-e', "--event-id", nargs="?", required=False,
                        help="Event ID to check")
    parser.add_argument('-c', '--count', help="number of objects to search", default="100")
    parser.add_argument("-a", "--check-all", action="store_true", default=False,
                        help="Check all events")
    parser.add_argument("-o", "--only-suspicious", action="store_true", default=False,
                        help="Include benign events")
    parser.add_argument("-u", "--update-event", action="store_true", default=False,
                        help="Check all events")

    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                        level=logging.INFO)
    opts = parser.parse_args()

    count = int(opts.count)

    if not opts.event_id and not opts.check_all:
        parser.print_help()
        return

    # FIXME: event_type
    locator = TransitionLocator(event_type="")
    if opts.event_id:
        is_transition = locator.check_transition_by_id(opts.event_id, update=opts.update_event)
        print(is_transition)
        return

    if opts.check_all:
        locator.locate_events(limit=count, update=opts.update_event, suspicious_only=opts.only_suspicious)
        return


if __name__ == "__main__":
    main()
