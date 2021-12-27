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
from grip.events.details_edges import EdgesDetails
from grip.events.details_moas import MoasDetails
from grip.events.details_submoas import SubmoasDetails
from grip.events.event import Event
from grip.events.pfxevent import PfxEvent


def create_dummy_event(event_type, tags=None, ts=None, tr_worthy=False, prefix="8.8.8.0/24", super_pfx="8.8.8.0/24", sub_pfx="8.8.8.8/32"):
    if ts is None:
        ts = 0
    event = Event(event_type=event_type, position="NEW", event_id="test-1", view_ts=ts)
    if event_type == "moas":
        details = MoasDetails(prefix=prefix, origins_set={15169, 12345}, old_origins_set={15169}, aspaths=[])
    elif event_type == "submoas":
        details = SubmoasDetails(super_pfx=super_pfx, sub_pfx=sub_pfx, super_origins={15169},
                                 sub_origins={15169, 12345},
                                 super_old_origins={15169}, sub_old_origins={15169}, sub_aspaths=[], super_aspaths=[])
    elif event_type == "defcon":
        details = DefconDetails(super_pfx=prefix, sub_pfx="8.8.8.8/32", origins_set={15169},
                                super_aspaths=[], sub_aspaths=[])
    elif event_type == "edges":
        details = EdgesDetails(prefix=prefix, as1=15169, as2=12345, aspaths_str="")
    else:
        assert False
    pfx_event = PfxEvent(event_type=event_type, position="NEW", view_ts=ts, details=details)
    pfx_event.traceroutes["worthy"] = tr_worthy
    if tags:
        # add all provided tags to the prefix events
        pfx_event.add_tags(tags)
    event.set_pfx_events([pfx_event])
    event.summary.update()
    return event
