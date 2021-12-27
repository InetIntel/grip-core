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

from grip.utils.bgp import aspaths_as_str, aspaths_from_str
from .details import PfxEventDetails


class SubmoasDetails(PfxEventDetails):

    def get_origin_fingerprint(self):
        return "%s=%s" % (
            "_".join(sorted(self._super_origins)),
            "_".join(sorted(self._sub_origins)),
        )

    def as_dict(self, incl_paths):

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
            "super_origins": list(self._super_origins),
            "sub_origins": list(self._sub_origins),
            "super_old_origins": list(self._super_old_origins),
            "sub_old_origins": list(self._sub_old_origins),
            "super_new_origins": list(self.get_super_new_origins()),
            "sub_new_origins": list(self.get_sub_new_origins()),
            # as paths
            "super_aspaths": super_paths,
            "sub_aspaths": sub_paths,
        }

    @staticmethod
    def from_dict(d):
        return SubmoasDetails(
            super_pfx=d["super_pfx"],
            sub_pfx=d["sub_pfx"],
            super_origins=set(d["super_origins"]),
            sub_origins=set(d["sub_origins"]),
            super_old_origins=set(d["super_old_origins"]),
            sub_old_origins=set(d["sub_old_origins"]),
            super_aspaths=aspaths_from_str(d["super_aspaths"]),
            sub_aspaths=aspaths_from_str(d["sub_aspaths"]),
        )

    def __init__(
        self,
        super_pfx,
        sub_pfx,
        super_origins,
        sub_origins,
        super_aspaths,
        sub_aspaths,
        super_old_origins=None,
        sub_old_origins=None,
    ):
        PfxEventDetails.__init__(self)

        if super_old_origins is None:
            super_old_origins = set()
        if sub_old_origins is None:
            sub_old_origins = set()

        assert isinstance(super_origins, set)
        assert isinstance(sub_origins, set)
        assert isinstance(super_old_origins, set)
        assert isinstance(sub_old_origins, set)
        assert isinstance(super_aspaths, list)
        assert isinstance(sub_aspaths, list)

        self._super_pfx = super_pfx
        self._sub_pfx = sub_pfx
        self._super_origins = super_origins
        self._sub_origins = sub_origins
        self._super_old_origins = super_old_origins
        self._sub_old_origins = sub_old_origins
        self._super_aspaths = super_aspaths
        self._sub_aspaths = sub_aspaths

        self._all_origins_set = self._super_origins.union(self._sub_origins)

        self._old_origins_set = self._super_old_origins.union(self._sub_old_origins)
        self._new_origins_set = self._all_origins_set - self._old_origins_set
        self._newcomer_pfxs = []

        if len(self._sub_origins - self._sub_old_origins) > 0:
            self._newcomer_pfxs.append(self._sub_pfx)
        if len(self._super_origins - self._super_old_origins) > 0:
            self._newcomer_pfxs.append(self._super_pfx)

    def get_current_origins(self):
        return self._all_origins_set

    def get_new_origins(self):
        return self._new_origins_set

    def get_super_old_origins(self):
        return self._super_old_origins

    def get_sub_old_origins(self):
        return self._sub_old_origins

    def get_super_new_origins(self):
        return self._super_origins - self._super_old_origins

    def get_sub_new_origins(self):
        return self._sub_origins - self._sub_old_origins

    def get_previous_origins(self):
        return self._old_origins_set

    def get_all_aspaths(self):
        aspaths = []
        aspaths.extend(self._super_aspaths)
        aspaths.extend(self._sub_aspaths)
        return aspaths

    def get_sub_aspaths(self):
        return self._sub_aspaths

    def get_super_aspaths(self):
        return self._super_aspaths

    def get_sub_origins(self):
        return self._sub_origins

    def get_super_origins(self):
        return self._super_origins

    def get_newcomer_prefixes(self):
        pfxs = []
        if len(self._sub_origins - self._sub_old_origins) > 0:
            pfxs.append(self._sub_pfx)
        if len(self._super_origins - self._super_old_origins) > 0:
            pfxs.append(self._super_pfx)
        return pfxs

    def is_sub_pfx_newcomer(self):
        """
        The self._newcomer_pfxs has most two items. If the origin of the subprefix is the newcomer,
        the self._newcomer_pfxs[0] == self._sub_pfx; if the origin of the super-prefix is the newcomer,
        the self._newcomer_pfxs[0] == self._super_pfx.

        If there is no newcomers, len(self._newcomer_pfxs) == 0.
        """
        if not self._newcomer_pfxs:
            return False
        return list(self._newcomer_pfxs)[0] == self._sub_pfx

    def is_super_pfx_newcomer(self):
        if not self._newcomer_pfxs:
            return False
        return list(self._newcomer_pfxs)[0] == self._super_pfx

    def extract_attackers_victims(self):
        """
        Extact attacker(s) and victims(s)

        Newcomer more specific:
            Attackers:
            - newcomer(s) for the sub-prefix

            Victims:
            - oldcomer(s) for super-prefix
            - newcomer(s) if there is no oldcomers in the current origins for super-prefix
        Newcomer less specific:
            Attackers:
            - newcomer(s) for the super-prefix

            Victims:
            - oldcomer(s) for sub-prefix
            - newcomer(s) if there is no oldcomers in the current origins for sub-prefix
        """

        if self.is_sub_pfx_newcomer():
            # newcomer more specific (or both)

            # potential attacker
            attackers = self._sub_origins - self._sub_old_origins

            # potential victim
            super_current_old_origins = self._super_origins.intersection(
                self._super_old_origins
            )
            if super_current_old_origins:
                # some current origins of the super-prefix are oldcomers
                victims = super_current_old_origins
            else:
                # no current origins are oldcomers, the victims are the all current super-prefix origins
                victims = self._super_origins
        else:
            # newcomer less specific

            # potential attacker
            attackers = self._super_origins - self._super_old_origins

            # potential victim
            sub_current_old_origins = self._sub_origins.intersection(self._sub_old_origins)
            if sub_current_old_origins:
                # some current origins of the sub-prefix are oldcomers
                victims = sub_current_old_origins
            else:
                # no current origins are oldcomers, the victims are the all current sub-prefix origins
                victims = self._sub_origins

        return attackers, victims

    def set_old_origins(self, super_old_origins, sub_old_origins):
        assert isinstance(super_old_origins, set)
        assert isinstance(sub_old_origins, set)

        self._super_old_origins = super_old_origins
        self._sub_old_origins = sub_old_origins

        self._old_origins_set = self._super_old_origins.union(self._sub_old_origins)
        self._new_origins_set = self._all_origins_set - self._old_origins_set

        if len(self._sub_origins - self._sub_old_origins) > 0:
            self._newcomer_pfxs.append(self._sub_pfx)
        if len(self._super_origins - self._super_old_origins) > 0:
            self._newcomer_pfxs.append(self._super_pfx)

    def get_prefixes(self):
        return [self._super_pfx, self._sub_pfx]

    def get_super_pfx(self):
        return self._super_pfx

    def get_sub_pfx(self):
        return self._sub_pfx

    def get_prefix_of_interest(self):
        return self._sub_pfx

