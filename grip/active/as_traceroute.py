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
import itertools

from grip.redis import Pfx2AsHistorical
from grip.utils.data.reserved_prefixes import ReservedPrefixes


class AsTracerouteDriver:
    """
    AS Tracereoute Driver, converting traceroute IP hops to AS paths
    """

    def __init__(self):
        self.ixp_dataset = None
        self.pfx_origin_dataset = Pfx2AsHistorical()
        self.reserved_pfxs = ReservedPrefixes()

    @staticmethod
    def __preprocess_trace(trace):
        hops = trace["hops"]
        return [
            (hops[key]["addr"], int(key), hops[key]["ttl"])
            for key in sorted(trace["hops"].keys())
        ]

    def fill_as_traceroute_results(self, traceroute_results, view_ts):
        """do as traceroute and fill the results structure"""

        for result_dict in traceroute_results:
            hops = self.__preprocess_trace(result_dict)
            result_dict["as_traceroute"] = self.as_traceroute(hops, view_ts)

    def __ip_to_as(self, ip, view_ts):
        """
        Convert a single IP to an AS number
        """
        if ip == "*" or self.reserved_pfxs.is_reserved(ip):
            asn = "*"
        else:
            # TODO check IXP data
            _, asns_info = self.pfx_origin_dataset.lookup("{}/32".format(ip), max_ts=view_ts)
            historical_asns = set(itertools.chain.from_iterable([info[2] for info in asns_info]))
            asn = " ".join(historical_asns)

        if asn is None:
            asn = "*"

        return asn

    def as_traceroute(self, hops, view_ts):
        """
        Convert traceroute IP hops to AS hops.

        hops: list of (IP, RTT) pairs
        """

        as_hops = [(ip, self.__ip_to_as(ip, view_ts)) for (ip, _, _) in hops]
        aspath = []
        prev_origins = ""
        for (ip, asn) in as_hops:
            if prev_origins == "":
                # first time, cannot be "*"
                if asn != "*":
                    aspath.append(asn)
            elif prev_origins == asn:
                # remove continuous duplicate ASNs
                pass
            elif len(aspath) >= 3 and aspath[-2] == "*" and aspath[-3] == asn:
                # if we are at A,*,A case, does not add the new "A" to the path
                pass
            elif asn in aspath:
                # loop avoidance, ignore later hops
                # TODO: may need to go back and discuss the caida_method with the team
                pass
            else:
                # good case
                aspath.append(asn)
            prev_origins = asn

        return aspath
