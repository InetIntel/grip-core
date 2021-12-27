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
import gzip
import json
import logging
import os
from enum import Enum
from pathlib import Path
from unittest import TestCase

from radix import Radix


class RpkiValidationStatus(str, Enum):
    VALID = "VALID"
    UNKNOWN = "UNKNOWN"
    INVALID_AS = "INVALID_AS"
    INVALID_LENGTH = "INVALID_LENGTH"


class RpkiUtils:
    def __init__(self, datadir):
        self.datadir = datadir
        self.radix = None
        self.currend_ts = None

    def _load_roas(self, roas):
        self.radix = Radix()
        for roa in roas:
            pfx = roa["prefix"]
            if ":" in pfx:
                # skip ipv6 prefixes for now
                continue
            node = self.radix.add(pfx)
            if "roas" not in node.data:
                node.data["roas"] = []
            node.data["roas"].append(roa)

    def _load_paths(self):
        """
        Scan all files in data dir and load existing file paths into memory for quick searching.
        :return:
        """
        ts_paths_map = {}
        for path in Path(self.datadir).rglob("roas.*.json.gz"):
            path = str(path)
            ts = int(path.split("/")[-1].split(".")[2])
            ts_paths_map[ts] = path
        return ts_paths_map

    def update_ts(self, ts: int, load_data=True):
        """
        Load ROAs data by unix timestamp.

        :param ts:
        :return:
        """
        # load all available data file paths
        ts_paths_map = self._load_paths()

        try:
            closest_ts = max([k for k in ts_paths_map.keys() if k <= ts])
        except ValueError:
            # found no timestamp that is before the given timestamp
            logging.error("no available RPKI ROA data found for time {}".format(ts))
            return False

        # debug info: check if we've found the exact match by time
        if closest_ts != ts:
            logging.info("exact data match for {} not found, closest data at {} is used".format(ts, closest_ts))

        # check if we've loaded the file already
        if closest_ts == self.currend_ts:
            logging.info("data already loaded for {}, skipping".format(ts))

        # load file
        data_dict = json.load(gzip.open(ts_paths_map[closest_ts], 'rt', encoding='UTF-8'))
        roas = data_dict["roas"]
        if load_data:
            self._load_roas(roas)
        self.currend_ts = closest_ts

        return True

    def validated_origins(self, pfx):
        """
        Get all validated origins for the given prefix.
        :param pfx:
        :return:
        """
        nodes = self.radix.search_covering(pfx)
        origins = set()

        for node in nodes:
            for roa in node.data["roas"]:
                asn = int(roa["asn"].lstrip("AS"))
                maxlength = roa["maxLength"]
                if maxlength >= int(pfx.split("/")[1]):
                    origins.add(asn)
        return origins

    def validate_prefix_origin(self, pfx, origin):
        """
        Validate a given prefix-origin pair.

        The implementation follows description of RFC6483:
        https://tools.ietf.org/rfc/rfc6483.txt

               Route    matching  non-matching
          Prefix   AS->   AS         AS
           V           +---------+---------+
          Non-         | unknown | unknown |
          Intersecting |         |         |
                       +---------+---------+
          Covering     | unknown | unknown |
          Aggregate    |         |         |
                       +---------+---------+
          match ROA    | valid   | invalid |
          prefix       |         |         |
                       +---------+---------+
          More         |         |         |
          Specific     | invalid | invalid |
          than ROA     |         |         |
                       +---------+---------+

                      Route's Validity State

        A route validity state is defined by the following procedure:

        1. Select all valid ROAs that include a ROAIPAddress value that
           either matches, or is a covering aggregate of, the address
           prefix in the route.  This selection forms the set of
           "candidate ROAs".

        2. If the set of candidate ROAs is empty, then the procedure stops
           with an outcome of "unknown" (or, synonymously, "not found", as
           used in [BGP-PFX]).

        3. If the route's origin AS can be determined and any of the set
           of candidate ROAs has an asID value that matches the origin AS
           in the route, and the route's address prefix matches a
           ROAIPAddress in the ROA (where "match" is defined as where the
           route's address precisely matches the ROAIPAddress, or where
           the ROAIPAddress includes a maxLength element, and the route's
           address prefix is a more specific prefix of the ROAIPAddress,
           and the route's address prefix length value is less than or
           equal to the ROAIPAddress maxLength value), then the procedure
           halts with an outcome of "valid".

        4. Otherwise, the procedure halts with an outcome of "invalid".

        :param pfx:
        :param origin:
        :return:
        """
        origin = int(origin)
        pfx_length = int(pfx.split("/")[1])

        # find candidate ROAs
        from itertools import chain
        if self.radix is None:
            return RpkiValidationStatus.UNKNOWN
        candidate_roas = list(chain.from_iterable([n.data["roas"] for n in self.radix.search_covering(pfx)]))

        # if no matching ROA for the prefix found, the status is UNKNOWN
        if not candidate_roas:
            return RpkiValidationStatus.UNKNOWN

        # collect all ROAs for the given prefix by the origins
        # prefixes in these ROAs are either the prefix itself or its super-prefixes
        roas_by_origin = []
        for roa in candidate_roas:
            asn = int(roa["asn"].lstrip("AS"))
            maxlength = roa["maxLength"]
            if asn == origin:
                if maxlength >= pfx_length:
                    # if the current roa has matching origin and prefix length, the given pair is valid
                    return RpkiValidationStatus.VALID
                roas_by_origin.append(asn)

        # if we reached here, the given pair is invalid.

        # no ROAs matches the given prefix belongs to the given origin (but there are ROAs for from origins)
        if not roas_by_origin:
            return RpkiValidationStatus.INVALID_AS

        # if reaches here,  there is some ROAs by the origin for one a covering prefix of the given prefix,
        # but the most specific allowed prefix length is smaller than the current prefix's length
        return RpkiValidationStatus.INVALID_LENGTH


class Test(TestCase):

    def test_valid_roas(self):
        validator = RpkiUtils(None)
        roas = [
            {
                "prefix": "1.1.0.0/16",
                "asn": "AS1234",
                "maxLength": 16
            },
            {
                "prefix": "1.1.0.0/16",
                "asn": "AS5678",
                "maxLength": 20
            },
            {
                "prefix": "1.2.0.0/16",
                "asn": "AS1234",
                "maxLength": 20
            },
        ]
        validator._load_roas(roas)

        # exact match, multiple ROAs for the same prefix
        self.assertEqual(RpkiValidationStatus.VALID, validator.validate_prefix_origin("1.1.0.0/16", 1234))
        # the other AS who is also valid
        self.assertEqual(RpkiValidationStatus.VALID, validator.validate_prefix_origin("1.1.0.0/16", 5678))

        # within maxlength
        self.assertEqual(RpkiValidationStatus.VALID, validator.validate_prefix_origin("1.2.0.0/18", 1234))

    def test_invalid_roas(self):
        validator = RpkiUtils(None)
        roas = [
            {
                "prefix": "1.2.0.0/16",
                "asn": "AS1234",
                "maxLength": 20
            },
            {
                "prefix": "1.2.0.0/24",
                "asn": "AS1234",
                "maxLength": 25
            },
        ]
        validator._load_roas(roas)

        # exact match, wrong ASN
        self.assertEqual(RpkiValidationStatus.INVALID_AS, validator.validate_prefix_origin("1.2.0.0/16", 4321))

        # within maxlength, wrong ASN
        self.assertEqual(RpkiValidationStatus.INVALID_AS, validator.validate_prefix_origin("1.2.0.0/18", 4321))

        # correct AS, wrong length (in between two valid length ranges)
        self.assertEqual(RpkiValidationStatus.INVALID_LENGTH, validator.validate_prefix_origin("1.2.0.0/22", 1234))

        # more specific than max length
        self.assertEqual(RpkiValidationStatus.INVALID_LENGTH, validator.validate_prefix_origin("1.2.0.0/21", 1234))

        # more specific than max length and wrong AS
        self.assertEqual(RpkiValidationStatus.INVALID_AS, validator.validate_prefix_origin("1.2.0.0/21", 4321))

    def test_unknown_roas(self):
        validator = RpkiUtils(None)
        roas = [
            {
                "prefix": "1.2.0.0/16",
                "asn": "AS1234",
                "maxLength": 20
            }
        ]
        validator._load_roas(roas)

        # same ASN, different prefix
        self.assertEqual(RpkiValidationStatus.UNKNOWN, validator.validate_prefix_origin("8.1.0.0/16", 1234))

        # different ASN, different prefix
        self.assertEqual(RpkiValidationStatus.UNKNOWN, validator.validate_prefix_origin("9.1.0.0/16", 4321))

        # partially covered by some ROA
        self.assertEqual(RpkiValidationStatus.UNKNOWN, validator.validate_prefix_origin("1.0.0.0/8", 1234))
        self.assertEqual(RpkiValidationStatus.UNKNOWN, validator.validate_prefix_origin("1.0.0.0/8", 4567))

    def test_data_file_searching(self):
        this_path = os.path.dirname(__file__)
        validator = RpkiUtils("{}/rpki-test-data".format(this_path))

        # exact match
        self.assertTrue(validator.update_ts(1617796500, False))
        self.assertEqual(1617796500, validator.currend_ts)

        # slightly in the future, match current file
        self.assertTrue(validator.update_ts(1617796520, False))
        self.assertEqual(1617796500, validator.currend_ts)

        # slightly in the past, match previous file
        self.assertTrue(validator.update_ts(1617796420, False))
        self.assertEqual(1617753600, validator.currend_ts)

        # before the earliest file, no match, currently loaded file remain the same.
        self.assertFalse(validator.update_ts(1617753500, False))
        self.assertEqual(1617753600, validator.currend_ts)
