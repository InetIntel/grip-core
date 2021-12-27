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
from grip.events.details_defcon import DefconDetails
from grip.tagger.tags import tagshelper
from .common import get_previous_origins
from .tagger import Tagger


class DefconTagger(Tagger):
    OUTDATED = False

    def __init__(self, options=None):
        super(DefconTagger, self).__init__(
            name="defcon",
            file_regex=r"^subpfx-defcon\.(\d+)\.events\.gz$",
            options=options,
        )

    def tag_pfxevent(self, pfxevent):
        """
            Classify the defcon in legitimate and suspicious events
        """
        tags = set()
        details = pfxevent.details
        assert isinstance(details, DefconDetails)

        # query redis to get previous origins
        super_old_origins, OUTDATED = get_previous_origins(
            pfxevent.view_ts, pfxevent.details.get_super_pfx(), self.datasets, self.in_memory)
        if OUTDATED:
            pfxevent.add_tags([tagshelper.get_tag("outdated-info")])
        pfxevent.details.set_old_origins(super_old_origins)

        tags.update(
            self.methodology.tag_prefixes(
                prefixes=details.get_prefixes(),
            ),
            self.methodology.tag_asns(
                previous_origins_set=details.get_previous_origins(),
                current_origins_set=details.get_current_origins(),
            ),
            # NOTE @mw: we don't know which prefix is newcomer prefix, correct?
            # tag_newcomer_pfxs(
            #     newcomer_pfxs=event.details.get_newcomer_prefixes(),
            #     super_pfx=event.details.super_pfx,
            #     sub_pfx=event.details.sub_pfx,
            #     tags_helper=self.methodology.tags_helper
            # ),
            self.methodology.tag_rpki(
                pfx_event=pfxevent
            ),
            self.methodology.tag_common_hops(
                super_aspaths=details.get_super_aspaths(),
                sub_aspaths=details.get_sub_aspaths(),
            ),
            self.methodology.tag_hegemony(
                ts=pfxevent.view_ts,
                paths=details.get_sub_aspaths(),
            ),
            self.methodology.tag_defcon(details)
        )
        # finally, check there is no tags generated
        tags.update(self.methodology.tag_notags(tags))

        # based on the tags, extract prefix event's traceroute-worthiness
        do_traceroute, worthy_tags = tagshelper.check_tr_worthy(pfxevent.event_type, tags)
        pfxevent.traceroutes["worthy"] = do_traceroute
        pfxevent.traceroutes["worthy_tags"] = worthy_tags

        pfxevent.add_tags(tags)

