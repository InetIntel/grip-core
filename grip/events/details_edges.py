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
from collections import OrderedDict

from grip.utils.bgp import *
from .details import PfxEventDetails


class EdgesDetails(PfxEventDetails):
    def get_previous_origins(self):
        pass

    def get_origin_fingerprint(self):
        return "{}_{}".format(self._as1, self._as2)

    def as_dict(self, incl_paths=True):
        paths = ""
        paths_with_newedge = ""
        if incl_paths:
            paths = decompress_aspaths_str(self._aspaths_compressed)
            paths_with_newedge = aspaths_as_str(self.get_aspaths_with_newedge())

        return {
            "prefix": self._prefix,
            "as1": self._as1,
            "as2": self._as2,
            "aspaths": paths,
            "aspaths_with_newedge": paths_with_newedge,
        }

    @staticmethod
    def from_dict(d):
        return EdgesDetails(
            prefix=d["prefix"],
            as1=d["as1"],
            as2=d["as2"],
            aspaths_str=d["aspaths"],
        )

    def __init__(
            self,
            as1,
            as2,
            prefix,
            aspaths_str: str,
    ):
        PfxEventDetails.__init__(self)

        assert isinstance(as1, int) and isinstance(as2, int)
        assert isinstance(aspaths_str, str)

        self._as1 = as1
        self._as2 = as2
        self._edgeid = "{}-{}".format(as1, as2)
        self._prefix = prefix
        self._origins = self._extract_origins(aspaths_str)
        self._aspaths_compressed = compress_aspaths_str(aspaths_str)

    def get_ases(self):
        return {self._as1, self._as2}

    def get_aspaths_with_newedge(self):
        """
        get all as paths that has the new edge in them
        """
        edge_str = "{},{}".format(self._as1, self._as2)
        edge_rev_str = "{},{}".format(self._as2, self._as1)
        newedge_paths = []
        for path in self.get_dedup_as_paths():
            path_str = ",".join([str(asn) for asn in path])
            if edge_str in path_str or edge_rev_str in path_str:
                newedge_paths.append(path)
        return newedge_paths

    def get_as_paths(self):
        return aspaths_from_str(decompress_aspaths_str(self._aspaths_compressed))

    def get_prefixes(self):
        return [self._prefix]

    def get_prefix_of_interest(self):
        return self._prefix

    def get_new_origins(self):
        return set()

    def get_current_origins(self):
        return self._origins

    def get_as1(self):
        return self._as1

    def get_as2(self):
        return self._as2

    def get_edgeid(self):
        return self._edgeid

    def get_dedup_as_paths(self):
        """
        Get as paths without duplicate elements
        :return:
        """
        return [list(OrderedDict.fromkeys(path)) for path in self.get_as_paths()]

    def get_edge_positions_on_paths(self):
        """
        Retrieve the positions of the edge in question on all the paths they're in.
        The position is represented as a pair of count of hops before and after the edge on paths
        :return: list of pairs of integers
        """
        positions = []
        for path in self.get_dedup_as_paths():
            try:
                index_1 = path.index(str(self._as1))
                index_2 = path.index(str(self._as2))
                if abs(index_1 - index_2) == 1:
                    # the current path contains the new edge
                    # get the position
                    hops_before_edge = min(index_1, index_2)
                    hops_after_edge = len(path) - max(index_1, index_2) - 1
                    positions.append((hops_before_edge, hops_after_edge))
            except ValueError:
                pass
        return positions

    def _extract_origins(self, aspath_str):

        return {path[-1] for path in aspaths_from_str(aspath_str) if len(path) > 0}

    def extract_attackers_victims(self):
        """
        Potential victims are all the current origins.

        Potential attackers are the two ASNs of the edge.
        """
        victims = self.get_current_origins()

        attackers = {self._as1, self._as2} - victims
        return attackers, victims
