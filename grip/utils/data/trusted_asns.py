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
import unittest
from itertools import chain


class TrustedAsns(object):
    """
    Trusted ASNs: list of DDoS companies
    """

    def __init__(self):
        # TODO: consider merging friend's list here as well. FOr example, Verisign has a lot more ASNs than these four
        self.trusted_asn = {
            "Akamai": [20940, 16625],
            "CenturyLink": [209, 3561],
            "CloudFlare": [13335],
            "DOSarrest": [19324],
            "F5 Networks": [55002],
            "Incapsula": [19551],
            "Level 3": [3549, 3356, 11213, 10753],
            "Neustar ": [7786, 12008, 19905],
            "Verisign": [26415, 30060, 7342, 16838],
            "Prolexic": [32787],
            "ARBOR": [20052],
            "neustar": [19905, 19907, 19910, 19911, 32978, 32979],
            "nexusguard": [454974],
            "radware": [15823, 48851],
            "LLNW-Limelight Networks": [12411, 22822, 23059, 23135, 23164, 25804],
            "OpenDNS": [36692],
            "LIQUID-WEB-INC": [32244, 30844],
            "staminus": [25761],
            "voxility": [3223, 39743],
            "Micron21": [38880],
            "rackspace": [19994, 10532, 12200, 19994, 27357, 36247],
            "DDOS-GUARD": [57724],
            "AZT-CLOUDDDOS": [53587],
            "DOSARREST": [19324],
            "ZENEDGE": [393676]
        }

        self.list_trusted_asn = set(chain.from_iterable(self.trusted_asn.values()))

    def is_asn_trusted(self, asn):
        """
        Return True if `asn` is in the trusted list.
        :param asn:
        :return:
        """
        if not str(asn).isdigit():
            return False

        return int(asn) in self.list_trusted_asn

    def list_asn(self):
        return list(self.list_trusted_asn)


class TestTrustedAsns(unittest.TestCase):
    def setUp(self):
        self.trusted = TrustedAsns()

    def test_is_trusted(self):
        self.assertTrue(self.trusted.is_asn_trusted(19324))
        self.assertFalse(self.trusted.is_asn_trusted(1598))
