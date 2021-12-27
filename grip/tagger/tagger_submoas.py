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
from grip.events.details_submoas import SubmoasDetails
from grip.tagger.common import get_previous_origins
from grip.tagger.methods import asn_should_keep
from grip.tagger.tags import tagshelper
from .tagger import Tagger


class SubMoasTagger(Tagger):
    OUTDATED = False

    def __init__(self, options=None):
        super(SubMoasTagger, self).__init__(
            name="submoas",
            file_regex=r"^subpfx-submoas\.(\d+)\.events\.gz$",
            options=options,
        )

    def tag_pfxevent(self, pfxevent):

        tags = set()
        details = pfxevent.details
        assert isinstance(details, SubmoasDetails)

        # query redis to get previous origins, also add outdated-info if data is outdated
        # TODO: cache this
        super_old_origins, outdated_super = get_previous_origins(
            pfxevent.view_ts, pfxevent.details.get_super_pfx(), self.datasets, self.in_memory)
        sub_old_origins, outdated_sub = get_previous_origins(
            pfxevent.view_ts, pfxevent.details.get_sub_pfx(), self.datasets, self.in_memory)
        if outdated_sub or outdated_super:
            pfxevent.add_tags([tagshelper.get_tag("outdated-info")])
        pfxevent.details.set_old_origins(super_old_origins=super_old_origins, sub_old_origins=sub_old_origins)

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
            ))

        # clean up origins, remove private and astrans origins
        previous_origins = {asn for asn in previous_origins if asn_should_keep(asn)}
        current_origins = {asn for asn in current_origins if asn_should_keep(asn)}
        new_origins = current_origins - previous_origins

        # extract attackers and victims
        attackers, victims = details.extract_attackers_victims()
        attackers = {asn for asn in attackers if asn_should_keep(asn)}
        victims = {asn for asn in victims if asn_should_keep(asn)}

        tags.update(
            self.methodology.tag_prefixes(
                prefixes=details.get_prefixes(),
            ),
            # TODO: change origins -> attackers/victims
            self.methodology.tag_historical(
                prefix=details.get_newcomer_prefixes(),
                new_origins_set=new_origins,
                in_memory=self.in_memory,
            ),
            # TODO: change origins -> attackers/victims
            self.methodology.tag_fat_finger(
                current_origins_set=current_origins,
                previous_origins_set=previous_origins,
                pfx_event=pfxevent,
                typo_pfx=details.get_sub_pfx(),
                in_memory=self.in_memory,
            ),
            self.methodology.tag_rpki(
                pfx_event=pfxevent
            ),
            # TODO: change origins -> attackers/victims
            self.methodology.tag_paths(
                current_origins_set=current_origins,
                previous_origins_set=previous_origins,
                as_paths=details.get_all_aspaths(),
            ),
            self.methodology.tag_relationships(
                attacker_origins_set=attackers,
                victim_origins_set=victims,
            ),
            self.methodology.tag_hegemony(
                ts=pfxevent.view_ts,
                paths=details.get_sub_aspaths(),
            ),
            self.methodology.tag_submoas(details),
        )

        # finally, check there is no tags generated
        tags.update(self.methodology.tag_notags(tags))

        # based on the tags, extract prefix event's traceroute-worthiness
        do_traceroute, worthy_tags = tagshelper.check_tr_worthy(pfxevent.event_type, tags)
        pfxevent.traceroutes["worthy"] = do_traceroute
        pfxevent.traceroutes["worthy_tags"] = worthy_tags

        pfxevent.add_tags(tags)
