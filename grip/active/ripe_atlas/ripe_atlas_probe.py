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
import os
import pickle
import random
import time

from ripe.atlas.cousteau import ProbeRequest
from ripe.atlas.cousteau.exceptions import APIResponseError

from grip.utils.data.asrank import AsRankUtils

DEBUG = False


class Probe(object):

    def __init__(self, pid, iso2, asn):
        self.probe_id = pid
        self.asn = asn
        self.country_code = iso2

    def as_dict(self):
        return {
            "probe_id": self.probe_id,
            "asn": self.asn,
            "country_code": self.country_code,
        }

    def as_json(self):
        return json.dumps(self.as_dict())


class ProbesCache:
    """Atlas probes information cache server"""

    # cache will be valid for one hour
    CACHE_VALID_SECONDS = 3600

    def __init__(self, event_type):
        self.asn_probes_map = {}
        self.updated_time = None
        self.pickle_name = "/tmp/grip-active/probes/online-probes-{}.pickle".format(event_type)

    def update_cache_if_needed(self):
        if self.updated_time is None or int(time.time()) - self.updated_time > self.CACHE_VALID_SECONDS:
            # if the cache is too old, update the cache again
            logging.info("updating online Atlas probes information at {}".format(time.time()))
            self._collect_probes()

    def _collect_probes(self):
        tmp_asn_probes_map = {}

        if self.updated_time is None:
            # first time, try load pickle first
            try:
                pickle_file = open(self.pickle_name, "rb")
                self.asn_probes_map = pickle.load(pickle_file)
                self.updated_time = int(time.time())
                pickle_file.close()
                logging.info("online probes pickle loaded from {}".format(self.pickle_name))
                return
            except:
                logging.info("online probes pickle file doesn't exist, do update now.")
                pass

        try:
            __version__ = "2.2.3"
            agent = "RIPE Atlas Tools (Magellan) {}".format(__version__)
            probes = ProbeRequest(return_objects=True, user_agent=agent, **{"status": 1})
            count = 0
            for probe in probes:
                if probe.status != "Connected":
                    logging.info("probe is not connected %s!", probe)
                count += 1
                p = Probe(pid=probe.id, iso2=probe.country_code, asn=probe.asn_v4)
                if p.asn not in tmp_asn_probes_map:
                    tmp_asn_probes_map[p.asn] = set()
                tmp_asn_probes_map[p.asn].add(p)
                if DEBUG and count % 100 == 0:
                    print("{}/{} {}".format(count, probes.total_count, p.as_json()))
        except APIResponseError as e:
            logging.warning("RIPE Atlas API response error: {}".format(e))
            logging.warning("stop collecting atlas probes due to previous error.")

        self.asn_probes_map = tmp_asn_probes_map
        self.updated_time = int(time.time())

        self.dump_pickle()

    def get_online_probes(self, asns):
        self.update_cache_if_needed()
        online_asns = [asn for asn in asns if asn in self.asn_probes_map]
        return [(k, list(self.asn_probes_map[k])) for k in online_asns]

    def dump_pickle(self):
        logging.info("dump online probes pickle file to %s" % self.pickle_name)
        tmp_dir = "/tmp/grip-active/probes"
        try:
            os.makedirs(tmp_dir)
        except:
            pass
        pickle.dump(self.asn_probes_map, open(self.pickle_name, "wb"))


class ProbeSelector(object):
    """probe selection procedure"""

    def __init__(self, event_type):
        self.event_type = event_type
        self.timestamp = 0
        self.asrank = None
        self.probe_server = ProbesCache(event_type)

    def update_asrank(self, timestamp):
        """update asrank instance based on the timestamp"""

        if timestamp != self.timestamp:
            self.asrank = AsRankUtils(max_ts=timestamp)
            self.timestamp = timestamp
        return True

    def pick_adjacent_probes(self, asn, threshold=None, max_hops=5):
        """
        pick probes from adjacent ASes
        FIXME: the implementation seems too complicated for clarity and ease of understanding
        """

        if self.asrank is None:
            # it is possible that ASRank is not ready
            return None

        hops = 0
        probes_selected = []
        process_group = list()
        process_group.append((asn, 'target'))
        visited, queue = set(), [(asn, 'target'), '*']

        while queue and (hops < max_hops):
            if queue[0] == '*':
                # reach the end of the queue
                break

            # pop the first item in queue
            vertex, tag = queue.pop(0)

            if vertex not in visited:
                # First, check whether we visited the AS before for the same pfx_event
                # Among target ASes, we would find the common adjacent ASes for them.
                # If already ran measurements for the AS, we do not need to run again.
                if vertex != asn:
                    if vertex in visited:
                        continue
                    else:
                        visited.add(vertex)

                adjacent_ases = self.asrank.get_neighbor_ases(vertex)
                queue.extend([(a, 'customers') for a in adjacent_ases['customers'] if a != ""])
                process_group.extend([(a, 'customer') for a in adjacent_ases['customers'] if a != ""])
                if tag == 'providers' or tag == 'target':
                    queue.extend([(a, 'peers') for a in adjacent_ases['peers'] if a != ""])
                    process_group.extend([(a, 'peer') for a in adjacent_ases['peers'] if a != ""])
                if tag == 'target':
                    queue.extend([(a, 'providers') for a in adjacent_ases['providers'] if a != ""])
                    process_group.extend([(a, 'provider') for a in adjacent_ases['providers'] if a != ""])

            if len(process_group) > 20 or queue[0] == '*':
                # if the queue is the end of a probing group or enough ases in the list

                finished = False
                for as_type in ['target', 'customer', 'peer', 'provider']:
                    # process ases in different groups with explicit order above
                    ases = [int(asn) for (asn, t) in process_group if t == as_type and asn.isdigit()]
                    if not ases:
                        continue
                    # NOTE: below is an older version of getting online probes.
                    # This query is replaced with newer probe server implementation
                    # probes_with_asn = self._get_online_probes(ases, threshold - len(probes_selected))
                    probes_with_asn = self.probe_server.get_online_probes(ases)

                    for _, probes in probes_with_asn:
                        probes_selected.append(probes[random.randint(0, len(probes) - 1)])

                    if len(probes_selected) >= threshold:
                        # Stop when we reach the threshold for the number of probes per AS
                        finished = True
                        break
                process_group = []

                if finished:
                    break

                if queue[0] == '*':
                    queue.pop(0)
                    hops += 1
                    if queue:
                        queue.append('*')

        probes_selected = probes_selected[:threshold]
        # probe_ids = list(set([x.probe_id for x in probes_selected]))
        # probes_ripe_format = ','.join(str(x) for x in probe_ids)

        # NOTE: consider utilize other information from the probe
        return probes_selected
