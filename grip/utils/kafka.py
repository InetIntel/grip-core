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

import confluent_kafka

import grip.common


def _kafka_producer_delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.info('Message delivery failed: {}'.format(err))
    else:
        logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


class KafkaHelper:

    def __init__(self,
                 brokers=grip.common.KAFKA_BROKERS,
                 message_max_bytes=10485760,
                 ):
        """
        initialization of KafkaHelper
        :param brokers: kafka brokers, defaults to grip.common.KAFKA_BROKERS
        :param message_max_bytes: maximum bytes per message to send, defaults to 10MB
        """
        self.brokers = brokers
        self.max_bytes = message_max_bytes

        self.producer = None
        self.consumer = None

        self.default_producer_topic = None
        self.default_consumer_topic = None

    def init_producer(self, topic):
        """
        initialize kafka producer. allow setting default producer topic.

        :param topic: default producer topic, nullable
        """
        self.producer = confluent_kafka.Producer({
            "bootstrap.servers": self.brokers,
            "message.max.bytes": self.max_bytes,
            # "socket.keepalive.enable": True
        })
        if topic is not None:
            logging.info("kafka producer default topic set to be: {}".format(topic))
            self.default_producer_topic = topic

    def init_consumer(self, topics, group_id, offset="earliest", autocommit=False):
        """
        initialize kafka consumer.

        :param topics: topics to subscribe to
        :param group_id: consumer group, nullable
        :param offset: consumer offset, default to "earliest
        :param autocommit: whether to automatically commit message offsets to kafka
        """

        assert (isinstance(topics, list))

        from confluent_kafka import OFFSET_BEGINNING
        self.consumer = confluent_kafka.Consumer({
            "bootstrap.servers": self.brokers,
            "group.id": group_id,
            "auto.offset.reset": offset,
            # "max.poll.interval.ms": 600000,  # 10 minutes between polls
            # do not automatically commit offsets to kafka
            # users to make sure to  call consumer.commit() manually
            "enable.auto.commit": autocommit,
        })
        self.consumer.subscribe(topics)
        logging.info("subscribing to kafka topic: {}; group: {}, broker: {}, offset: {}".format(",".join(topics), group_id, self.brokers, offset))

    def commit_offset(self):
        assert (self.consumer is not None)
        assert (isinstance(self.consumer, confluent_kafka.Consumer))
        self.consumer.commit()

    def poll(self, interval=5):
        """
        poll from consumer
        :param interval: seconds to value for interval, default set to 5 seconds
        """
        assert (self.consumer is not None)
        assert (isinstance(self.consumer, confluent_kafka.Consumer))
        return self.consumer.poll(interval)

    def produce(self, value_str, topic=None, flush=False, report=False):
        """
        produce message to kafka topic.
        if the topic not set, use the default producer kakfa set during producer's initialization.
        also allow synchronized producing if performance is not a major concern.

        :param report:
        :param value_str: the message string
        :param topic: topic to send the message to, nullable
        :param flush: boolean for setting if doing synchronized producing
        """
        assert (self.producer is not None)
        assert (isinstance(self.producer, confluent_kafka.Producer))
        assert (topic is not None or self.default_producer_topic is not None)

        if topic:
            tmp_topic = topic
        else:
            tmp_topic = self.default_producer_topic

        logging.debug("producing kafka message to {}: {}".format(tmp_topic, value_str))
        callback_func = None
        if report:
            callback_func = _kafka_producer_delivery_report
        self.producer.produce(topic=tmp_topic, value=value_str, callback=callback_func)

        if flush:
            # see performance punishment for sync producer:
            # https://github.com/edenhill/librdkafka/wiki/FAQ#why-is-there-no-sync-produce-interface
            self.producer.flush()

    def flush(self):
        """
        flush kafka producer.
        also read: https://github.com/edenhill/librdkafka/wiki/FAQ#why-is-there-no-sync-produce-interface
        """
        assert (self.producer is not None)
        assert (isinstance(self.producer, confluent_kafka.Producer))
        logging.debug("flushing kafka producer")
        self.producer.flush()


def drain_topic(topics, group):
    print("draining topic %s for group: %s" % (topics, group))
    kafka = KafkaHelper()
    kafka.init_consumer(topics, group, autocommit=True, offset="earliest")

    count = 0
    while True:
        msg = kafka.poll(10)
        if msg is None:
            # print("draining finished")
            break
        assert (isinstance(msg, confluent_kafka.Message))
        if not msg.error():
            print("message %d" % count)
            print("\t%s" % msg.value().decode("utf-8")[:1000])
            count += 1


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s", level=logging.INFO)
    for event_type in ["submoas", "moas", "edges", "defcon"]:
        # draining kafka consumers messages
        # clear driver's
        drain_topic(["observatory-tagger-%s" % event_type],
                    "observatory-driver-%s" % event_type)
        # clear collector's
        drain_topic(["observatory-driver-%s" % event_type],
                    "observatory-collector-%s" % event_type)
        # clear inference's
        drain_topic(["observatory-tagger-%s" % event_type, "observatory-collector-%s" % event_type],
                    "observatory-inference-%s" % event_type)
