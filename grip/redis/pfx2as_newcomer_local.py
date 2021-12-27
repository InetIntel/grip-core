#!/usr/bin/env python

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
import os

import wandio
from radix import Radix
from itertools import chain
from glob import glob
from ..utils.fs import fs_generate_file_list, fs_get_timestamp_from_file_path

# TODO allow this to be specified by caller?
# XXX what about historical data, is that relevant here?
PATH="/data/bgp/live/pfx-origins/production/"

class Pfx2AsNewcomerLocal:
    """
    Localized version of the pfx2as lookup for 24 hour window
    The data is coming from directly loading pfx-origin files from the file system into memory.
    """

    def __init__(self, exact_match=True, datafile=None):
        """
        Constructor for newcomer dataset in-memory version.

        :param exact_match:
        :param datafile: path to a pfx-to-origin data file.
        """
        # initialize class-wide variables
        self.exact_match = exact_match
        self.pfx_origin_files = {}
        self.sorted_file_ts = []

        if self.exact_match:
            self.rtree = None
        else:
            self.rtree = Radix()
        self.as2pfx_dict = {}
        self.pfx2as_dict = {}
        self.file_timestamp = 0  # current loaded file timestamp
        self.view_timestamp = 0  # current view timestamp for the loaded data
        self.datafile = None

        if datafile:
            self.datafile = datafile

    def _load_files_list(self):
        # load all pfx-origin files in first
        logging.info("Updating list of pfx-origins files")

        files = fs_generate_file_list(PATH)

        for f in files:
            timestamp = fs_get_timestamp_from_file_path(f)
            self.pfx_origin_files[timestamp] = f


    def _init_data(self):
        if self.exact_match:
            self.rtree = None
        else:
            self.rtree = Radix()
        self.as2pfx_dict = {}
        self.pfx2as_dict = {}
        self.file_timestamp = 0  # current loaded file timestamp
        self.view_timestamp = 0  # current view timestamp for the loaded data

    def _load_pfx_file(self, path):
        # clear previous cached data
        self._init_data()

        logging.info("loading pfx2as mappings into memory from %s" % path)
        self.file_timestamp = 0
        try:
            with wandio.open(path) as fh:
                for line in fh:
                    # 1476104400|115.116.0.0/16|4755|4755|STABLE
                    timestamp, prefix, old_asn, new_asn, label = line.strip().split("|")
                    timestamp = int(timestamp)
                    if self.file_timestamp == 0:
                        self.file_timestamp = timestamp
                    elif timestamp != self.file_timestamp:
                        raise ValueError("Multiple timestamps in one file", path)

                    if label == "REMOVED" or ":" in prefix:
                        # do not insert prefixes that are no longer announced
                        # we also do not (currently) support IPv6 prefixes
                        continue

                    # convert the ip to a binary string
                    if not self.exact_match:
                        self.rtree.add(prefix)
                    self.pfx2as_dict[prefix] = new_asn

                    # save as2pfx data into dictionary
                    if new_asn not in self.as2pfx_dict:
                        self.as2pfx_dict[new_asn] = set()
                    self.as2pfx_dict[new_asn].add(prefix)

        except IOError as e:
            logging.error("Could not read pfx-origin file '%s'" % path)
            logging.error("I/O error: %s" % e.strerror)
            return
        except ValueError as e:
            logging.error("mapping ValueError!")
            logging.error(e.args)
            return
        logging.info("...loading pfx2as mappings finished")

    def check_and_load_data_from_timestamp(self, timestamp):
        assert (isinstance(timestamp, int))

        if self.datafile:
            if self.file_timestamp == 0:
                self._load_pfx_file(self.datafile)
        else:
            if timestamp == self.view_timestamp:
                # we have loaded corresponding data for the timestamp
                return

            if not self.pfx_origin_files:
                # load file list if not loaded yet
                self._load_files_list()
                self.sorted_file_ts = sorted(self.pfx_origin_files.keys())

            most_recent_ts = max([ts for ts in self.sorted_file_ts if ts < timestamp])

            # we need to load a new pfx_origins file
            self._load_pfx_file(self.pfx_origin_files[most_recent_ts])
            self.view_timestamp = int(timestamp)

    # noinspection PyUnusedLocal
    def get_most_recent_timestamp(self, view_ts):
        return self.file_timestamp

    def get_timestamp(self):
        return self.file_timestamp

    # noinspection PyUnusedLocal
    def lookup(self, prefix, exact_match=True, max_ts=None):
        """
        Queries redis for pfx2as mappings for the last 24 hours
        Returns:
        - the queried prefix or closest super-prefix (if exact_match not set)
        - if a timestamp is specified, returns the most recent (ASN, timestamp)
        (before the timestamp) otherwise, a list of tuple (ASN, timestamp).

        i.e., ('8.8.8.0/24', [('15169', 1473120000.0)]
        """

        if self.file_timestamp == 0:
            # uninitialized data
            raise ValueError("data not loaded in memory yet")

        match = prefix  # default to the current prefix as matched prefix
        if not self.exact_match:
            # find the longest matches
            match = self.rtree.search_best(prefix)
            if match is None:
                return None, []
            match = match.prefix
        else:
            # exact match case
            if prefix not in self.pfx2as_dict:
                # no exact match found, return
                return None, []

        # match is found, the data is guaranteed exist
        asns = self.pfx2as_dict[match]
        return match, [(asns, self.file_timestamp)]

    # noinspection PyUnusedLocal
    def lookup_as(self, asn, max_ts=None, latest=False):
        """
        Queries redis for as2pfx mappings for the last 24 hours
        """
        # return the list
        # return [self._extract_res(res) for res in results]
        if self.file_timestamp == 0:
            # uninitialized data
            raise ValueError("data not loaded in memory yet")

        if asn not in self.as2pfx_dict:
            return []

        return [(",".join(self.as2pfx_dict[asn]), self.file_timestamp)]
