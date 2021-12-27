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

import os
import unittest

import yaml


class OrgFriends:
    friend_asn_sets = None
    friend_asn_dict = None
    friend_org_sets = None

    def __init__(self):
        self.friend_asn_sets = []
        self.friend_org_sets = []
        self.friend_asn_dict = {}
        self.load_friends_list()

    def load_friends_list(self):
        datafile = os.path.dirname(os.path.realpath(__file__)) + "/" + "org_friends.yaml"
        with open(datafile, 'r') as stream:
            try:
                records = yaml.safe_load(stream)
                for record in records:
                    if "ases" in record:
                        ases_set = set([str(asn) for asn in record["ases"]])
                        self.friend_asn_sets.append(ases_set)
                        for asn in ases_set:
                            if asn not in self.friend_asn_dict:
                                self.friend_asn_dict[asn] = []
                            self.friend_asn_dict[asn].append(ases_set)
                    if "orgs" in record:
                        orgs_set = set(record["orgs"])
                        self.friend_org_sets.append(orgs_set)
            except yaml.YAMLError as exc:
                print(exc)

    def are_friends(self, asn1, asn2):
        """
        check if two ases are friends
        :param asn1: as number in string or number
        :param asn2: as number in string or number
        :return:
        """

        asn1 = str(asn1)
        asn2 = str(asn2)

        if asn1 in self.friend_asn_dict and asn2 in self.friend_asn_dict:
            for ases_set in self.friend_asn_dict[asn1]:
                if asn2 in ases_set:
                    return True

        return False


class TestFriends(unittest.TestCase):
    def setUp(self):
        self.friends = OrgFriends()

    def test_are_friends(self):
        self.assertTrue(self.friends.are_friends("6646", "22610"))
        self.assertFalse(self.friends.are_friends("6646", "22611"))
        self.assertTrue(self.friends.are_friends("58519", "134419"))
        self.assertTrue(self.friends.are_friends("721", "13"))

