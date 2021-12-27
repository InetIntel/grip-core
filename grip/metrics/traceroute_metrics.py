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


from grip.common import ACTIVE_MAX_PFX_EVENTS, ACTIVE_MAX_EVENT_ASES, ACTIVE_MAX_PROBES_PER_TARGET


class TracerouteMetrics:
    """metrics and extra information collected for each event"""

    def __init__(self,
                 max_pfx_events=ACTIVE_MAX_PFX_EVENTS, max_event_ases=ACTIVE_MAX_EVENT_ASES, max_vps_per_event_as=ACTIVE_MAX_PROBES_PER_TARGET,
                 tr_worthy=False, tr_worthy_tags=None, tr_skipped=False, tr_skip_reason="",
                 selected_vp_cnt=0, selected_unique_vp_cnt=0, total_event_as_cnt=0, selected_event_as_cnt=0,
                 tr_worthy_pfx_event_cnt=0, selected_pfx_event_cnt=0,
                 tr_request_cnt=0, tr_request_failure_cnt=0,
                 ):
        # thresholds
        self.max_pfx_events = max_pfx_events  # how many prefixes are we willing to trace per event?
        self.max_event_ases = max_event_ases  # how many prefixes are we willing to trace per event?
        self.max_vps_per_event_as = max_vps_per_event_as  # how many prefixes are we willing to trace per event?

        # event-level
        self.tr_worthy = tr_worthy
        self.tr_skipped = tr_skipped
        if tr_worthy_tags is None:
            tr_worthy_tags = []
        self.tr_worthy_tags = set()
        for tags in tr_worthy_tags:
            self.tr_worthy_tags.add(tuple(tags))
        self.tr_skip_reason = tr_skip_reason

        self.selected_vp_cnt = selected_vp_cnt
        self.selected_unique_vp_cnt = selected_unique_vp_cnt
        self.total_event_as_cnt = total_event_as_cnt
        self.selected_event_as_cnt = selected_event_as_cnt

        # pfx_event-level
        self.tr_worthy_pfx_event_cnt = tr_worthy_pfx_event_cnt
        self.selected_pfx_event_cnt = selected_pfx_event_cnt

        self.tr_request_cnt = tr_request_cnt
        self.tr_request_failure_cnt = tr_request_failure_cnt

    def update_tags(self, pfx_events):
        for e in pfx_events:
            if e.traceroutes["worthy"]:
                self.tr_worthy_tags.add(tuple(e.traceroutes["worthy_tags"]))

    @staticmethod
    def from_dict(d):
        return TracerouteMetrics(**d)
