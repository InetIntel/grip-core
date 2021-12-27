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

import collections

import wandio

from grip.events.details_edges import EdgesDetails
from grip.tagger.tags import tagshelper
from .tagger import Tagger


class EdgesTagger(Tagger):
    OUTDATED = False

    def __init__(self, options=None):
        super(EdgesTagger, self).__init__(
            name="edges",
            file_regex=r"^edges\.(\d+)\.events\.gz$",
            options=options,
        )

    def find_bidirectional_new_edge(self, consumer_filename):
        """
        Used to create "bi_edges_info", which is a dictionary that for each new edge seen by
        the consumer has value [d1, d2] where d1==1 indicates that the link has been seen in
        the exact direction and d2 indicates the opposite direction.
        """
        edges_temp = collections.defaultdict(lambda: [0, 0])
        with wandio.open(consumer_filename) as fh:
            # for each line in the file
            for line in fh:
                # ignore commented lines
                if line.startswith("#"):
                    continue
                # consider only new events
                if "NEW" in line:
                    columns = line.strip().split("|")
                    edge_key = columns[1]
                    aspaths = columns[4].split(":")
                    (as1, as2) = edge_key.split('-')
                    as1_as2 = "%s %s" % (as1, as2)
                    as2_as1 = "%s %s" % (as2, as1)
                    if edges_temp[edge_key] != [1, 1]:
                        for aspath in aspaths:
                            if not len(aspath):
                                continue
                            if as1_as2 in aspath:
                                edges_temp[edge_key][0] = 1
                            elif as2_as1 in aspath:
                                edges_temp[edge_key][1] = 1
        return edges_temp

    def update_datasets(self, ts, consumer_filename=None):
        super(EdgesTagger, self).update_datasets(ts, consumer_filename)
        self.datasets["bi_edges_info"] = self.find_bidirectional_new_edge(consumer_filename)

    def tag_pfxevent(self, pfxevent):
        tags = set()
        details = pfxevent.details
        assert isinstance(details, EdgesDetails)

        tags.update(
            self.methodology.tag_asns(
                previous_origins_set=details.get_ases(),
                current_origins_set=details.get_ases(),
            ),
            self.methodology.tag_prefixes(
                prefixes=details.get_prefixes()
            ),
            # TODO: previous-origins nto checked.
            # self.methodology.tag_rpki(
            #     pfx_event=pfxevent
            # ),
            self.methodology.tag_hegemony(
                ts=pfxevent.view_ts,
                paths=details.get_aspaths_with_newedge(),
            ),
            self.methodology.tag_edges(details)
        )

        # finally, check there is no tags generated
        tags.update(self.methodology.tag_notags(tags))

        # based on the tags, extract prefix event's traceroute-worthiness
        do_traceroute, worthy_tags = tagshelper.check_tr_worthy(pfxevent.event_type, tags)
        pfxevent.traceroutes["worthy"] = do_traceroute
        pfxevent.traceroutes["worthy_tags"] = worthy_tags

        pfxevent.add_tags(tags)

