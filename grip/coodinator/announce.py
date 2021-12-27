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
import signal
import sys
import time

import confluent_kafka
import wandio

import grip.common

DEFAULT_KAFKA_TOPIC = "grip-production.announcements"
DEFAULT_TOPIC_OFFSET = "latest"


class Announcement(object):

    def __init__(self, type, sender_type, sender_name, timestamp=None):
        self.ann_type = type
        if timestamp is None:
            timestamp = time.time()
        self.timestamp = timestamp
        self.sender_type = sender_type
        self.sender_name = sender_name

    def __str__(self):
        return self.as_txt()

    def __repr__(self):
        return self.as_json()

    @staticmethod
    def from_txt(txt):
        raise NotImplementedError

    def as_dict(self):
        return {
            "type": self.ann_type,
            "timestamp": self.timestamp,
            "sender_type": self.sender_type,
            "sender_name": self.sender_name,
        }

    def as_json(self):
        return json.dumps(self.as_dict())

    def as_txt(self):
        return "%s %s %s %s" % (self.ann_type, self.timestamp,
                                self.sender_type, self.sender_name)


class DatabaseAnnouncement(Announcement):

    def __init__(self, sender_name, timestamp, sender_type="tagger"):
        super(DatabaseAnnouncement, self).__init__(
            type="db",
            sender_type=sender_type,
            sender_name=sender_name,
            timestamp=timestamp
        )

    @staticmethod
    def from_txt(txt):
        # sender_name timestamp
        fields = txt.split(" ")
        if len(fields) != 2:
            raise ValueError("DatabaseAnnouncement text format: "
                             "'sender_name timestamp'")
        return DatabaseAnnouncement(
            sender_name=fields[0],
            timestamp=fields[1]
        )

    def as_txt(self):
        return "%s %s %s" % (self.ann_type, self.sender_name, self.timestamp)


class SwiftAnnouncement(Announcement):

    def __init__(self, sender_type, sender_name, container, object, timestamp=None):
        self.container = container
        self.object = object
        super(SwiftAnnouncement, self).__init__(
            type="swift",
            sender_type=sender_type,
            sender_name=sender_name,
            timestamp=timestamp
        )

    @staticmethod
    def from_txt(txt):
        # sender_type sender_name container object [timestamp]
        # consumer $consumer $container $object $timestamp
        fields = txt.split(" ")
        if len(fields) != 4 and len(fields) != 5:
            raise ValueError("SwiftAnnouncement text format: "
                             "'sender_type sender_name container object [timestamp]'")
        timestamp = None
        if len(fields) == 5:
            timestamp = fields[-1]
            # fields = fields[1:]
        return SwiftAnnouncement(
            sender_type=fields[0],
            sender_name=fields[1],
            container=fields[2],
            object=fields[3],
            timestamp=timestamp
        )

    def as_dict(self):
        sdict = super(SwiftAnnouncement, self).as_dict()
        sdict["container"] = self.container
        sdict["object"] = self.object
        return sdict

    def as_json(self):
        return json.dumps(self.as_dict())

    def as_txt(self):
        return "%s %s %s" % (super(SwiftAnnouncement, self).as_txt(),
                             self.container, self.object)

    @property
    def uri(self):
        return "swift://%s/%s" % (self.container, self.object)


class FileAnnouncement(Announcement):

    def __init__(self, sender_type, sender_name, path, timestamp=None):
        self.path = path
        super(FileAnnouncement, self).__init__(
            type="file",
            sender_type=sender_type,
            sender_name=sender_name,
            timestamp=timestamp
        )

    @staticmethod
    def from_txt(txt):
        # sender_type sender_name path [timestamp]
        # consumer $consumer $path $timestamp
        fields = txt.split(" ")
        if len(fields) != 3 and len(fields) != 4:
            raise ValueError("FileAnnouncement text format: "
                             "'sender_type sender_name path [timestamp]'")
        timestamp = None
        if len(fields) == 4:
            timestamp = fields[-1]
            # fields = fields[1:]
        return FileAnnouncement(
            sender_type=fields[0],
            sender_name=fields[1],
            path=fields[2],
            timestamp=timestamp
        )

    def as_dict(self):
        sdict = super(FileAnnouncement, self).as_dict()
        sdict["path"] = self.path
        return sdict

    def as_json(self):
        return json.dumps(self.as_dict())

    def as_txt(self):
        return "%s %s" % (super(FileAnnouncement, self).as_txt(), self.path)

    @property
    def uri(self):
        return self.path


ANNOUNCEMENT_CLASSES = {
    "db": DatabaseAnnouncement,
    "swift": SwiftAnnouncement,
    "file": FileAnnouncement,
}


# factory functions for building announcements:
def build(type, **kwargs):
    if type not in ANNOUNCEMENT_CLASSES:
        raise ValueError("Invalid announcement type: '%s'" % type)
    return ANNOUNCEMENT_CLASSES[type](**kwargs)


def build_from_txt(type, txt):
    if type not in ANNOUNCEMENT_CLASSES:
        raise ValueError("Invalid announcement type: '%s'" % type)
    return ANNOUNCEMENT_CLASSES[type].from_txt(txt)


def build_from_json(json_str):
    return build(**json.loads(json_str))


class Listener:

    def __init__(self,
                 group,
                 sender_type=None,
                 sender_name=None,
                 brokers=grip.common.KAFKA_BROKERS,
                 offset=DEFAULT_TOPIC_OFFSET,
                 topic=DEFAULT_KAFKA_TOPIC,
                 auto_commit=True,
                 ):
        self.sender_type = sender_type
        self.sender_name = sender_name
        self.auto_commit = auto_commit
        self.kc = confluent_kafka.Consumer({
            "bootstrap.servers": brokers,
            "group.id": group,
            "default.topic.config": {"auto.offset.reset": offset},
            "enable.auto.commit": auto_commit,
        })
        self.kc.subscribe([topic])
        logging.info("Listener started")

    def _match_filters(self, announcement):
        if (self.sender_type is not None and
            self.sender_type != announcement.sender_type) or \
                (self.sender_name is not None and
                 self.sender_name != announcement.sender_name):
            return False
        return True

    def commit_offset(self):
        self.kc.commit()

    def listen(self, limit=None):
        shutdown = {"count": 0}

        def _stop_handler(_signo, _stack_frame):
            logging.info("Caught signal, shutting down at next opportunity")
            shutdown["count"] += 1
            if shutdown["count"] > 3:
                logging.warning("Caught %d signals, shutting down NOW" % shutdown["count"])
            sys.exit(0)

        signal.signal(signal.SIGTERM, _stop_handler)
        signal.signal(signal.SIGINT, _stop_handler)

        count = 0
        while True:
            if limit is not None and count == limit:
                return
            if shutdown["count"] > 0:
                logging.info("Shutting down")
                break
            msg = self.kc.poll(5)
            if msg is None:
                continue
            if not msg.error():
                text = msg.value().decode("utf-8")
                print(text)
                ann = build_from_json(text)
                if not self._match_filters(ann):
                    continue
                count += 1
                yield ann
            elif msg.error().code() != confluent_kafka.KafkaError._PARTITION_EOF:
                logging.error("Unhandled Kafka error: %s" % msg.error())


class Announcer:

    def __init__(self,
                 brokers=grip.common.KAFKA_BROKERS,
                 topic=DEFAULT_KAFKA_TOPIC
                 ):
        self.topic = topic
        self.kc = confluent_kafka.Producer({
            "bootstrap.servers": brokers,
        })
        logging.info("Announcer started")

    def __del__(self):
        wait = 60
        if len(self.kc):
            logging.info("Waiting 60s for %d queued announcements to be sent" %
                         len(self.kc))
        while wait > 0:
            self.kc.flush(5)
            wait -= 5

    def announce(self, announcement):
        json_str = announcement.as_json()
        logging.info("Producing announcement '%s' to topic '%s'", json_str, self.topic)
        self.kc.produce(self.topic, json_str)
        self.kc.flush()


def listen_and_announce(callback, listener):
    assert (isinstance(listener, Listener))
    announcer = Announcer()
    for in_ann in listener.listen():
        out_anns = callback(in_ann)
        if out_anns is None:
            out_anns = []
        for ann in out_anns:
            announcer.announce(ann)
        if not listener.auto_commit:
            # if the listener is not set to autocommit when retrieving data from kakfa, we should commit the offset
            # manually here as shown below. if the autocommit is set to be True, then no need to call commit_offset()
            listener.commit_offset()


def main():
    parser = argparse.ArgumentParser(description="""
    Utilities for sending and receiving "announcements".
    """)

    mode_grp = parser.add_mutually_exclusive_group(required=True)
    mode_grp.add_argument('-l', "--listen", action="store_true",
                          help="Listen for announcements")
    mode_grp.add_argument('-a', "--announce", action="store_true",
                          help="Send announcement(s)")

    listen_grp = parser.add_argument_group("listener-specific options")
    listen_grp.add_argument('-c', '--count', nargs="?", type=int,
                            help="Exit after receiving n announcements")
    listen_grp.add_argument('-g', "--group", nargs="?",
                            help="Listener group to join")
    listen_grp.add_argument('-o', "--offset", nargs="?",
                            help="auto.offset.reset setting (earliest/latest)",
                            default=DEFAULT_TOPIC_OFFSET)
    listen_grp.add_argument('-s', "--sender-type", nargs="?",
                            help="Filter for announcements from the given "
                                 "sender type")
    listen_grp.add_argument('-n', "--sender-name", nargs="?",
                            help="Filter for announcements from the given "
                                 "sender name")

    kafka_grp = parser.add_argument_group("kafka options")
    kafka_grp.add_argument('-b', "--brokers", nargs="?",
                           help="Comma-separated list of Kafka brokers",
                           default=grip.common.KAFKA_BROKERS)
    kafka_grp.add_argument('-t', "--topic", nargs="?",
                           help="Kafka topic to use",
                           default=DEFAULT_KAFKA_TOPIC)

    ann_grp = parser.add_argument_group("announcer-specific options")
    ann_grp.add_argument('-f', "--file", nargs="?",
                         help="Read announcements from given file "
                              "(default: stdin)")

    parser.add_argument('-j', '--json', action="store_true",
                        required=False, help="Print JSON-formatted output")

    parser.add_argument('-v', '--verbose', action="store_true",
                        required=False, help='Verbose logging')

    opts = parser.parse_args()

    logging.basicConfig(level="DEBUG" if opts.verbose else "INFO",
                        format="%(asctime)s|%(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    if opts.listen:
        if not opts.group:
            parser.print_help()
            sys.stderr.write("\nERROR: --group must be set when listening\n")
            sys.exit(-1)
        listener = Listener(
            group=opts.group,
            sender_type=opts.sender_type,
            sender_name=opts.sender_name,
            brokers=opts.brokers,
            offset=opts.offset,
            topic=opts.topic
        )
        for ann in listener.listen(limit=opts.count):
            if opts.json:
                print(ann.as_json())
            else:
                print(ann)

    if opts.announce:
        announcer = Announcer(brokers=opts.brokers, topic=opts.topic)
        fh = wandio.open(opts.file) if opts.file else sys.stdin
        for line in fh:
            fields = line.strip().split(" ")
            # example:
            # file tagger moas /data/xxxx 10000000
            ann = build_from_txt(fields[0], " ".join(fields[1:]))
            announcer.announce(ann)
