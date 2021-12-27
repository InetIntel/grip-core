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
import time

import grip.utils.data.elastic


class OperationalEvent(object):
    """
    OperationalEvent object abstract events during operations, such as the start of tagger, driver, collector, etc.
    The data will be committed to ElasticSearch, and can be used for annotation by Grafana afterwards.

    This should be packaged into a CLI command and ship with the observatory package, allowing the systemd to use and
    commit information to elasticsearch.
    """

    def __init__(self, ops_type, message, tags, event_type, ops_ts, component, instance_name):
        if ops_ts is None:
            ops_ts = int(time.time())
        assert (isinstance(ops_ts, int))
        self.ops_ts = ops_ts
        self.ops_type = ops_type
        self.message = message
        self.tags = tags

        if instance_name is not None:
            # e.g. active-collector-defcon, tagger-defcon, inference-collector-moas
            fields = instance_name.split("-")

            if fields[-1] not in ["moas", "submoas", "defcon", "edges"]:
                self.event_type = "all"
                self.component = "-".join(fields)
            else:
                self.event_type = fields[-1]
                self.component = "-".join(fields[:-1])

        # can overwrite the inferred values
        if component:
            self.component = component
        if event_type:
            self.event_type = event_type

    def as_dict(self):
        return {
            "ops_ts": self.ops_ts,
            "ops_type": self.ops_type,
            "component": self.component,
            "event_type": self.event_type,
            "message": self.message,
            "tags": self.tags
        }

    def as_json_str(self):
        return json.dumps(self.as_dict())

    def get_id(self):
        return "operational-event-{}-{}-{}-{}".format(self.event_type, self.component, self.ops_ts, self.ops_type)


def main():
    parser = argparse.ArgumentParser(description="""
    BGP Observatory Operational Event Committer
    """)

    parser.add_argument("-t", "--time", required=False, help="Operation timestamp in unix time")
    parser.add_argument("-o", "--ops-type", required=True, help="Operation type (e.g. start, fail)")
    parser.add_argument("-T", "--tags", required=True, help="Operation tags (e.g. deployment, debug, test)")
    parser.add_argument("-e", "--event-type", required=False, help="Event type (e.g. moas, submoas, defcon, edges)")
    parser.add_argument("-c", "--component", required=False,
                        help="Component (e.g. tagger, driver, collector, inference)")
    parser.add_argument("-i", "--instance-name", required=False,
                        help="Systemd instance name (e.g. active-collector-defcon)")
    parser.add_argument("-m", "--message", required=False, help="Operation message")

    opts = parser.parse_args()

    logging.basicConfig(level="INFO",
                        format="%(asctime)s|%(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    logging.getLogger('elasticsearch').setLevel(logging.INFO)

    event = OperationalEvent(ops_type=opts.ops_type, ops_ts=opts.time, event_type=opts.event_type,
                             tags=opts.tags.split(","), message=opts.message, component=opts.component,
                             instance_name=opts.instance_name)

    esconn = grip.utils.data.elastic.ElasticConn()
    esconn.index_ops_event(event)


if __name__ == '__main__':
    main()
