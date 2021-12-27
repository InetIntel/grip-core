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

from grip.utils.bgp import *
from .details import PfxEventDetails


class DefconDetails(PfxEventDetails):

    def get_prefix_of_interest(self):
        return self._sub_pfx

    def get_origin_fingerprint(self):
        return "_".join(sorted(self._origins_set))

    def as_dict(self, incl_paths=True):

        super_paths = ""
        sub_paths = ""
        if incl_paths:
            super_paths = aspaths_as_str(self._super_aspaths)
            sub_paths = aspaths_as_str(self._sub_aspaths)

        return {
            # prefixes
            "super_pfx": self._super_pfx,
            "sub_pfx": self._sub_pfx,
            # origins
            "origins": list(self._origins_set),
            "old_origins": list(self._old_origins_set),
            "new_origins": list(self._new_origins_set),
            # as paths
            "super_aspaths": super_paths,
            "sub_aspaths": sub_paths,
        }

    @staticmethod
    def from_dict(d):
        return DefconDetails(
            super_pfx=d["super_pfx"],
            sub_pfx=d["sub_pfx"],
            origins_set=set(d["origins"]),
            old_origins_set=set(d["old_origins"]),
            super_aspaths=aspaths_from_str(d["super_aspaths"]),
            sub_aspaths=aspaths_from_str(d["sub_aspaths"]),
        )

    def __init__(
            self,
            super_pfx,
            sub_pfx,
            origins_set,
            super_aspaths,
            sub_aspaths,
            old_origins_set=None,
    ):
        PfxEventDetails.__init__(self)

        if old_origins_set is None:
            old_origins_set = set()

        assert (isinstance(origins_set, set))
        assert (isinstance(old_origins_set, set))
        assert (isinstance(super_aspaths, list))
        assert (isinstance(sub_aspaths, list))

        self._super_pfx = super_pfx
        self._sub_pfx = sub_pfx
        self._origins_set = origins_set
        self._old_origins_set = old_origins_set
        self._super_aspaths = super_aspaths
        self._sub_aspaths = sub_aspaths

        self._new_origins_set = self._origins_set - self._old_origins_set

    def get_current_origins(self):
        return self._origins_set

    def get_new_origins(self):
        return self._new_origins_set

    def get_previous_origins(self):
        return self._old_origins_set

    def get_all_aspaths(self):
        aspaths = []
        aspaths.extend(self._super_aspaths)
        aspaths.extend(self._sub_aspaths)
        return aspaths

    def get_super_aspaths(self):
        return self._super_aspaths

    def get_sub_aspaths(self):
        return self._sub_aspaths

    def set_old_origins(self, old_origins):
        assert (isinstance(old_origins, set))
        self._old_origins_set = old_origins

    def get_prefixes(self):
        return [self._super_pfx, self._sub_pfx]

    def get_super_pfx(self):
        return self._super_pfx

    def get_sub_pfx(self):
        return self._sub_pfx

    def extract_attackers_victims(self):
        return set(), self._origins_set
