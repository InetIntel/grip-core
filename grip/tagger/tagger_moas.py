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
from grip.events.details_moas import MoasDetails
from grip.tagger.common import get_previous_origins
from grip.tagger.methods import asn_should_keep
from grip.tagger.tags import tagshelper
from .tagger import Tagger


class MoasTagger(Tagger):
    """MOAS tagger"""

    tag_map = {}

    OUTDATED = False

    def __init__(self, options=None):
        super(MoasTagger, self).__init__(
            name="moas",
            file_regex=r"^moas\.(\d+)\.events\.gz$",
            options=options,
        )
        # static datasets: data do not change over time

    def tag_pfxevent(self, pfxevent):

        tags = set()
        details = pfxevent.details
        assert isinstance(details, MoasDetails)

        # query redis to get previous origins
        previous_origins, outdated = get_previous_origins(
            pfxevent.view_ts, pfxevent.details.get_prefix_of_interest(), self.datasets, self.in_memory)
        if outdated:
            tags.add(tagshelper.get_tag("outdated-info"))
        # note: previous origins might be empty (e.g., if this is a new prefix compared to the bgpview
        # that redis checked)
        details.set_old_origins(previous_origins)

        # get all data from details
        all_prefixes = details.get_prefixes()
        primary_prefix = details.get_prefix_of_interest()
        aspaths = details.get_aspaths()
        new_origins = details.get_new_origins()
        previous_origins = details.get_previous_origins()
        current_origins = details.get_current_origins()

        # tag asns with private/as_trans origins
        tags.update(
            self.methodology.tag_newcomer_origins(
                current_origins_set=current_origins,
                previous_origins_set=previous_origins,
            ),
            self.methodology.tag_asns(
                previous_origins_set=previous_origins,
                current_origins_set=current_origins,
            ),
            )
        # Clean up origins, remove private and astrans origins
        # NOTE: after this code block it's possible that current_origins and/or previous_origins are empty
        new_origins = {asn for asn in new_origins if asn_should_keep(asn)}
        previous_origins = {asn for asn in previous_origins if asn_should_keep(asn)}
        current_origins = {asn for asn in current_origins if asn_should_keep(asn)}

        # extract attackers and victims
        attackers, victims = details.extract_attackers_victims()
        attackers = {asn for asn in attackers if asn_should_keep(asn)}
        victims = {asn for asn in victims if asn_should_keep(asn)}

        # run other tagging functions
        tags.update(
            self.methodology.tag_prefixes(
                prefixes=all_prefixes,
            ),
            self.methodology.tag_historical(
                prefix=primary_prefix,
                new_origins_set=new_origins,
                in_memory=self.in_memory,
            ),
            self.methodology.tag_fat_finger(
                current_origins_set=current_origins,
                previous_origins_set=previous_origins,
                pfx_event=pfxevent,
                typo_pfx=primary_prefix,
                in_memory=self.in_memory,
            ),
            self.methodology.tag_rpki(
                pfx_event=pfxevent
            ),
            self.methodology.tag_paths(
                current_origins_set=current_origins,
                previous_origins_set=previous_origins,
                as_paths=aspaths,
            ),
            self.methodology.tag_relationships(
                attacker_origins_set=attackers,
                victim_origins_set=victims,
            ),
            self.methodology.tag_hegemony(
                ts=pfxevent.view_ts,
                paths=aspaths,
            ),
        )
        # finally, check there is no tags generated
        tags.update(self.methodology.tag_notags(tags))

        # based on the tags, extract prefix event's traceroute-worthiness
        do_traceroute, worthy_tags = tagshelper.check_tr_worthy(pfxevent.event_type, tags)
        pfxevent.traceroutes["worthy"] = do_traceroute
        pfxevent.traceroutes["worthy_tags"] = worthy_tags

        pfxevent.add_tags(tags)

