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

from grip.utils.bgp import aspaths_from_str, origins_from_str
from .details_defcon import DefconDetails
from .details_edges import EdgesDetails
from .details_moas import MoasDetails
from .details_submoas import SubmoasDetails
from .pfxevent import PfxEvent


def _should_skip_prefix_event(prefixes: list):
    for pfx in prefixes:
        # skip IPv6 prefix events
        if ":" in pfx:
            return True
        # skip event with super-large prefixes, i.e. larger than a /8
        if int(pfx.split("/")[1]) < 8:
            return True
    return False


class PfxEventParser:

    def __init__(self, event_type, is_caching=False):
        parsers = {
            "moas": self._parse_moas_line,
            "submoas": self._parse_submoas_line,
            "defcon": self._parse_defcon_line,
            "edges": self._parse_edges_line,
        }
        assert event_type in parsers
        self.parse_func = parsers[event_type]
        self.is_caching = is_caching

    def parse_line(self, line):
        return self.parse_func(line)

    def _parse_moas_line(self, line):
        try:
            (view_ts, prefix, position, aspathstr) = line.strip().split("|")
        except ValueError:
            # if error encountered. log error and continue parsing
            logging.error("Invalid MOAS event line: %s" % line)
            return None

        if _should_skip_prefix_event([prefix]):
            return None

        if position != "NEW" and position != "FINISHED":
            # logging.error("unknown pfxevent position: {}".format(position))
            return None

        aspaths = aspaths_from_str(aspathstr)
        origins_set = {aspath[-1] for aspath in aspaths}
        if position == "NEW" and not origins_set:
            logging.warning("unknown origins: {}".format(line))
            logging.warning("this is likely to be caused by only AS set segments existing in all AS paths")
            return None

        if self.is_caching:
            aspaths = []

        return PfxEvent(
            event_type="moas",
            view_ts=int(view_ts),
            position=position,
            details=MoasDetails(
                prefix=prefix,
                origins_set=origins_set,
                aspaths=aspaths
            ),
        )

    def _parse_edges_line(self, line):
        """
        Parse the input event string into PfxEvent object

        Example input:
            1475194200|10026-9381|NEW|2407:3100::/32|6939 2516 9381 9381 10118:7018 3257 9381 9381 10118:
            1475194200|9002-4761|FINISHED|

        :param line: input event string
        :return: PfxEvent object
        """
        # parse the line
        cols = line.strip().split("|")
        if len(cols) < 3:
            raise ValueError("Invalid NewEdge event line: %s" % line)

        aspathstr = ""
        if cols[2] == "FINISHED":
            try:
                (view_ts, edgeid, position, prefix) = cols
            except ValueError:
                raise ValueError("Invalid NewEdge event line: %s" % line)
        else:
            try:
                (view_ts, edgeid, position, prefix, aspathstr) = cols
            except ValueError:
                raise ValueError("Invalid NewEdge event line: %s" % line)

        if prefix and _should_skip_prefix_event([prefix]):
            return None

        if position != "NEW" and position != "FINISHED":
            return None

        (as1, as2) = edgeid.split("-")
        if self.is_caching:
            aspathstr=""

        return PfxEvent(
            event_type="edges",
            view_ts=int(view_ts),
            position=position,
            details=EdgesDetails(
                prefix=prefix,
                as1=int(as1),
                as2=int(as2),
                aspaths_str=aspathstr
            ),
        )

    def _parse_submoas_line(self, line):
        # parse the line
        # e.g.: 1475193600|2a00:a040::/32|2a00:a040:0:3::/64|NEW|12849|12849|<AS_PATHS>
        try:
            (view_ts,
             super_pfx, sub_pfx, position,
             super_origins_str, sub_origins_str,
             super_aspaths_str, sub_aspaths_str) = line.strip().split("|")
        except ValueError:
            raise ValueError("Invalid Submoas event line: %s" % line)

        if _should_skip_prefix_event([super_pfx, sub_pfx]):
            return None

        # sanity check first
        if position != "NEW" and position != "FINISHED":
            return None

        super_origins = origins_from_str(super_origins_str)
        sub_origins = origins_from_str(sub_origins_str)

        if self.is_caching:
            sub_aspaths = []
            super_aspaths = []
        else:
            sub_aspaths = aspaths_from_str(sub_aspaths_str)
            super_aspaths = aspaths_from_str(super_aspaths_str)

        return PfxEvent(
            event_type="submoas",
            view_ts=int(view_ts),
            position=position,
            details=SubmoasDetails(
                super_pfx=super_pfx,
                sub_pfx=sub_pfx,
                super_origins=super_origins,
                sub_origins=sub_origins,
                super_aspaths=super_aspaths,
                sub_aspaths=sub_aspaths,
            ),
        )

    def _parse_defcon_line(self, line):
        """
        Parse the input event string into PfxEvent object

        Example input:
            1475193600|117.157.0.0/16|117.157.69.0/24|NEW|9808|9808|6423 209 3356 58453 9808:
            1475196300|91.207.66.0/23|91.207.66.99/32|FINISHED||||

        :param line: input event string
        :return: PfxEvent object
        """

        try:
            (view_ts,
             super_pfx, sub_pfx, position,
             super_origins_str, sub_origins_str,
             super_aspaths_str, sub_aspaths_str) = line.strip().split("|")
        except ValueError:
            raise ValueError("Invalid Submoas event line: %s" % line)

        if _should_skip_prefix_event([super_pfx, sub_pfx]):
            return None

        # sanity check first
        if position != "NEW" and position != "FINISHED":
            return None

        # sub-prefix origins and as paths
        if self.is_caching:
            sub_aspaths = []
            super_aspaths = []
        else:
            sub_aspaths = aspaths_from_str(sub_aspaths_str)
            super_aspaths = aspaths_from_str(super_aspaths_str)

        # super-prefix origins and as paths
        super_origins = origins_from_str(super_origins_str)
        sub_origins = origins_from_str(sub_origins_str)
        if super_origins != sub_origins:
            raise ValueError("Super-prefix and sub-prefix origins differ (%s, %s)" %
                             (super_origins_str, sub_origins_str))

        return PfxEvent(
            event_type="defcon",
            view_ts=int(view_ts),
            position=position,
            details=DefconDetails(
                super_pfx=super_pfx,
                sub_pfx=sub_pfx,
                origins_set=set(super_origins),
                super_aspaths=super_aspaths,
                sub_aspaths=sub_aspaths,
            ),
        )
