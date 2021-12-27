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


class MoasDetails(PfxEventDetails):

    def extract_attackers_victims(self):
        return self._new_origins, self._old_origins

    def get_origin_fingerprint(self):
        return "_".join(sorted(self._origins))

    def get_prefix_of_interest(self):
        return self._prefix

    def as_dict(self, incl_paths):
        paths = ""
        if incl_paths:
            paths = aspaths_as_str(self._aspaths)
        return {
            "prefix": self._prefix,
            "origins": list(self._origins),
            "old_origins": list(self._old_origins),
            "new_origins": list(self.get_new_origins()),
            "aspaths": paths,
        }

    @staticmethod
    def from_dict(d):
        return MoasDetails(
            prefix=d["prefix"],
            origins_set=set(d["origins"]),
            old_origins_set=set(d["old_origins"]),
            aspaths=aspaths_from_str(d["aspaths"])
        )

    def __init__(
            self,
            prefix,
            origins_set,
            aspaths,
            old_origins_set=None,
    ):
        PfxEventDetails.__init__(self)

        # sanity check
        assert (isinstance(origins_set, set))
        assert (isinstance(aspaths, list))  # aspaths should be list of lists of integers

        if old_origins_set is None:
            old_origins_set = set()

        self._prefix = prefix
        self._origins = origins_set
        self._old_origins = old_origins_set
        self._aspaths = aspaths
        self._new_origins = self._origins - self._old_origins

    def get_current_origins(self):
        return self._origins

    def get_new_origins(self):
        return self._new_origins

    def get_previous_origins(self):
        return self._old_origins

    def get_aspaths(self):
        return self._aspaths

    def set_old_origins(self, old_origins):
        assert (isinstance(old_origins, set))
        self._old_origins = old_origins
        self._new_origins = self._origins - self._old_origins

    def get_prefixes(self):
        return [self._prefix]
