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

import radix
from netaddr import IPNetwork, IPAddress


class TargetIpGenerator:
    NO_PROBE_PFXS = (
        # https://tools.ietf.org/html/rfc6890
        "0.0.0.0/8",  # This host on this network RFC 1122
        "10.0.0.0/8",  # Private-Use RFC 1918
        "100.64.0.0/10",  # Shared Address Space RFC 6598
        "127.0.0.0/8",  # Loopback RFC 1122
        "169.254.0.0/16",  # Link Local RFC 3927
        "172.16.0.0/12",  # Private-Use RFC 1918
        "192.0.0.0/24",  # IETF Protocol Assignments RFC 6890
        "192.0.0.0/29",  # DS-Lite RFC 6333
        "192.0.2.0/24",  # Documentation (TEST-NET-1) RFC 5737
        "192.88.99.0/24",  # 6to4 Relay Anycast RFC 3068
        "192.168.0.0/16",  # Private-Use RFC 1918
        "198.18.0.0/15",  # Benchmarking RFC 2544
        "198.51.100.0/24",  # Documentation (TEST-NET-2) RFC 5737
        "203.0.113.0/24",  # Documentation (TEST-NET-3) RFC5737
        "240.0.0.0/4",  # Reserved RFC 1112
        "255.255.255.255/32",  # Limited Broadcast RFC 0919
        # IPv4 Multicast Address Assignments RFC 3171
        "224.0.0.0/4",  # Multicast   RFC 3171
        # https://conference.apnic.net/data/37/2014-02-27-prop-109_1393397866.pdf
        # https://www.apnic.net/policy/proposals/prop-109
        "1.0.0.0/24",  # APNIC Labs as Research Prefixes
        "1.1.1.0/24"  # APNIC Labs as Research Prefixes
    )

    def __init__(self):
        # initialize two prefix radix trees
        self.special_rtree = radix.Radix()
        self.pfxs_rtree = radix.Radix()

        # load special prefixes into `special_rtree`
        for pfx in self.NO_PROBE_PFXS:
            self.special_rtree.add(pfx)

    def add_pfx(self, pfx):
        """
        add prefix to the prefix tree, ignoring very long or short prefixes, also ignore special prefixes

        :param pfx: the prefix to be added to the tree
        :return: nothing
        """
        if pfx == "":
            logging.warning("empty string for prefix")
            return

        if self.special_rtree.search_best(pfx) is not None:
            # if the prefix is in the special prefix range
            # return without adding the prefix into tree
            return

        # ignore prefixes with short or long mask
        mask = int(pfx.split("/")[1])
        if mask < 7 or mask > 24:
            return

        self.pfxs_rtree.add(pfx)

    def get_probe_pfx_ip_map(self):
        """
        Check all prefixes in the current prefix tree and returns a dictionary
        that maps between the prefixes in the tree and the one of its IP (likely the first).

        In the cases where a super-prefix and sub-prefix both in the tree, it maps the IPs to the sub-prefix first.

        NOTE: if multiple-sub prefixes that fully cover a super-prefix, then the super-prefix will not be in the
        return dictionary

        :return: a prefix-to-ip map
        """
        pfx_ip_map = {}

        for node in self.pfxs_rtree:
            # loop through all the prefixes in the tree
            pfx = node.prefix

            # if an entry exists already, then
            # an IP has been already assigned
            if pfx in pfx_ip_map:
                continue

            # get the first and last ip of the network
            network = IPNetwork(pfx)
            ip_first = int(network.first)
            ip_last = int(network.last)
            assigned_ip = ip_first
            while assigned_ip <= ip_last:
                assigned_ip_str = str(IPAddress(assigned_ip+1))
                # Best-match search will return the longest matching prefix
                # for the current assigned_ip  (routing-style lookup)
                best_match_pfx = self.pfxs_rtree.search_best(assigned_ip_str).prefix

                # If the best match for the first IP is the same prefix, then
                # we have found an IP
                if best_match_pfx == pfx:
                    pfx_ip_map[pfx] = assigned_ip_str
                    break

                # if the best_match_pfx has not been assigned
                # an ip yet, then do it
                if best_match_pfx not in pfx_ip_map:
                    pfx_ip_map[best_match_pfx] = assigned_ip_str

                # in any case move the assigned_ip further:
                # move to the next prefix (having the same length as the
                # more specific found): the next network begins with
                # broadcast_long()) + 1 and has the same netmask
                # (note that the first host ip may not be broadcast + 1)
                matching_network = IPNetwork(best_match_pfx)
                mask = best_match_pfx.split("/")[1]
                next_network = IPNetwork(str(IPAddress(int(matching_network.broadcast) + 1)) + "/" + mask)
                assigned_ip = int(next_network.first)

        return pfx_ip_map

