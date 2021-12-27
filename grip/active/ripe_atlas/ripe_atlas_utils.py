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
import socket

from future.utils import iteritems
from netaddr import IPAddress, IPNetwork
from ripe.atlas.cousteau import (Traceroute, AtlasSource, AtlasCreateRequest, AtlasRequest)

from grip.redis import Pfx2AsHistorical
from grip.utils.data.ipmeta import get_ip_geo_location
from .ripe_atlas_msm import *


def check_ip_version(ip_addr):
    """
    Returns whether IP address in IPv4 or IPv6
    """
    try:
        socket.inet_aton(ip_addr)
        return 4
    except socket.error:
        # TODO: this address could also be invalid
        return 6


class RipeAtlasUtils:
    """RIPE Atlas traceroute class"""

    def __init__(self, key, num_probes=10):
        self.key = key
        self.num_probes = num_probes

    def create_request(self, target_ip, target_pfx, asn_probes_mapping, event_id):
        """
        Main driver function. Gets RIPE Atlas probes and issues traceroute request
        """

        jobs_succeeded = []
        jobs_failed = []

        logging.info("sending {} traceroute requests to RIPE Atlas...".format(len(asn_probes_mapping)))

        for asn, probes in iteritems(asn_probes_mapping):
            # Pick the given number of adjacent probes according to the given ASN
            probe_ids = [probe.probe_id for probe in probes]

            # When no Ripe Atlas probe found
            if not probe_ids:
                logging.warning("None of Atlas probes are located in the adjacent networks.")
                print("None of Atlas probes are located in the adjacent networks.")
                # MW: continue to allow other probes to get through
                continue

            description = event_id + ':' + str(asn)

            source = AtlasSource(type="probes",
                                 value=','.join(str(x) for x in probe_ids),
                                 requested=len(probe_ids))
            traceroute = Traceroute(af=check_ip_version(target_ip),
                                    target=target_ip,
                                    description=description,
                                    protocol="TCP", packets=1)
            atlas_request = AtlasCreateRequest(
                # NOTE: disable settting start_time, default to right-away
                #       This is useful during debug where there can be some delay between the measurement's creation and
                #       the actual request reaching Atlas. Error for requests with start_time in the past will be
                #       triggered otherwise
                # start_time=datetime.datetime.utcnow(),
                key=self.key,
                measurements=[traceroute],
                sources=[source],
                is_oneoff=True
            )

            # Making traceroute request to RIPE Atlas
            (is_success, response) = atlas_request.create()

            if is_success:
                jobs_succeeded.append(AtlasMeasurement(msm_id=response["measurements"][0],
                                                       probe_ids=probe_ids,
                                                       target_ip=target_ip,
                                                       target_asn=asn,
                                                       target_pfx=target_pfx,
                                                       request_error=[],
                                                       event_id=event_id
                                                       ))
            else:
                try:
                    logging.error("Can't create measurement: %s", response)
                    error = []
                    if 'errors' in response['error']:
                        error = [e['detail'] for e in response['error']['errors'] if 'detail' in e]
                    # When the RIPE request was failed, we would try again later.
                    # Let's keep the information for requesting
                    jobs_failed.append(AtlasMeasurement(msm_id=-1,
                                                        probe_ids=probe_ids,
                                                        target_ip=target_ip,
                                                        target_asn=asn,
                                                        target_pfx=target_pfx,
                                                        request_error=error,
                                                        event_id=event_id
                                                        ))
                except TypeError as e:
                    # capture "TypeError: string indices must be integers" error for `if 'errors' in response['error']:`
                    # print out the response message
                    logging.error("TypeError, response = {}".format(response))
                    raise e
            

        logging.info("\t{} requests succeeded, {} requests failed".format(len(jobs_succeeded), len(jobs_failed)))
        return jobs_succeeded, jobs_failed


def get_traceroute_status_map(msm_ids):
    """
    return traceroute status:
        id (integer): measurement ID
        status (integer): Numeric ID of this status (0: Specified, 1: Scheduled, 2: Ongoing,
            4: Stopped, 5: Forced to stop, 6: No suitable probes, 7: Failed, 8: Archived),
        name (string): Human-readable description of this status,
        when (string): When the measurement entered this status (not available for all statuses)
    """

    def divide_chunks(l, n):
        for idx in range(0, len(l), n):
            yield l[idx:idx + n]

    msm_map = {}
    for msm_lst in divide_chunks(msm_ids, 10):
        # 10 measurement per request
        request = AtlasRequest()
        request.url_path = "/api/v2/measurements/?id__in={0}".format(','.join([str(i) for i in msm_lst]))
        response = request.get()
        if response[0]:
            for result in response[1]["results"]:
                msm_map[result["id"]] = result["status"]

    return msm_map


def extract_atlas_response(responses, pfx_origin_db=None, target_pfx=None):
    """get response json for each job"""

    def lookup_origin(ip_addr, timestamp):
        """ Search the longest prefix match for the given IP address
        :param timestamp: timestamp for lookup
        :param ip_addr: ip address to lookup
        :return: the matching prefix and AS(es) owner or None
        """

        is_historical = isinstance(pfx_origin_db, Pfx2AsHistorical)

        if "/" not in ip_addr:
            # NOTE: it handles only IPv4 now
            ip_addr = ip_addr + "/32"

        prefix, origin_lst = pfx_origin_db.lookup(ip_addr, max_ts=timestamp)

        if prefix is None or not origin_lst:
            return "*"

        if is_historical:
            return " ".join(origin_lst[-1][2])
        else:
            return origin_lst[-1][0]

    traceroute_results = []
    for response in responses:  # response: list for each probe

        try:
            target_ip_reached = False
            target_pfx_reached = False

            all_replied_ips = [r["result"][0]["from"] for r in response["result"]
                               if "result" in r and "from" in r["result"][0]]

            if all_replied_ips:
                if all_replied_ips[-1] == response["dst_addr"]:
                    target_ip_reached = True
                try:
                    if target_pfx is not None and IPAddress(all_replied_ips[-1]) in IPNetwork(target_pfx):
                        target_pfx_reached = True
                except Exception as e:
                    logging.error(e)

            if "from" in response["result"][-1]["result"][0]:
                if response["result"][-1]["result"][0]["from"] == response["dst_addr"]:
                    target_ip_reached = True

            ip_hops = {}
            if 'result' in response:
                for hop in response["result"]:
                    hop_count = hop['hop']
                    resp = hop["result"][0]
                    ip_hops[hop_count] = {"rtt": 0.0, "addr": "*", "asn": "*", "ttl": 255}
                    if "rtt" in resp:
                        ip_hops[hop_count]["rtt"] = resp["rtt"]
                    if "ttl" in resp:
                        ip_hops[hop_count]["ttl"] = resp["ttl"]
                    if "from" in resp:
                        ip_hops[hop_count]["addr"] = resp["from"]
                    if ip_hops[hop_count]["addr"] != "*":
                        address = ip_hops[hop_count]["addr"]
                        if pfx_origin_db is not None:
                            ip_hops[hop_count]["asn"] = lookup_origin(address, response["timestamp"])

                        iplookup_res, country_code = get_ip_geo_location(address)
                        # logging.info("iplookup_res: {}".format(iplookup_res))
                        if iplookup_res:
                            lat, lg = iplookup_res
                            ip_hops[hop_count]["lat"] = lat
                            ip_hops[hop_count]["long"] = lg
                        if country_code:
                            ip_hops[hop_count]["country"] = country_code

            result = {
                "msm_id": response["msm_id"],
                "prb_id": response["prb_id"],

                "starttime": response["timestamp"],
                "endtime": response["endtime"],

                "src": response["from"],
                "dst": response["dst_addr"],

                "target_ip_reached": target_ip_reached,
                "target_pfx_reached": target_pfx_reached,
                "hops": ip_hops,
                "as_traceroute": []  # to be filled later
            }

            traceroute_results.append(result)
        except KeyError:
            logging.error("Result parsing error for response %s" % json.dumps(response))
        except TypeError:
            logging.error("Result parsing error for response %s" % json.dumps(response))

    return traceroute_results
