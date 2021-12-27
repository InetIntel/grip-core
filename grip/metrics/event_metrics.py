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


class EventMetrics:
    """
    metrics and extra information collected for each event
    """

    def __init__(self,
                 pfx_events_cnt=0, per_tag_cnt=None, total_tags_cnt=0, pfx_events_with_tr_cnt=0,
                 proc_time_tagger=0.0, proc_time_driver=0.0, proc_time_inference=0.0
                 ):
        self.pfx_events_cnt = pfx_events_cnt
        if per_tag_cnt is None:
            per_tag_cnt = []
        self.per_tag_cnt = per_tag_cnt
        self.total_tags_cnt = total_tags_cnt
        self.pfx_events_with_tr_cnt = pfx_events_with_tr_cnt

        # processing time in seconds
        self.proc_time_tagger = proc_time_tagger
        self.proc_time_driver = proc_time_driver
        self.proc_time_inference = proc_time_inference

    @staticmethod
    def from_dict(d):
        return EventMetrics(**d)

    def update(self, event):
        """
        Update metrics about the event before
        :return:
        """
        self.pfx_events_cnt = len(event.pfx_events)
        self.total_tags_cnt = len(event.summary.tags)
        self.pfx_events_with_tr_cnt = 0

        tag_cnt_dict = {}
        for pfx_event in event.pfx_events:
            try:
                msms = pfx_event.traceroutes["msms"]
                if any([msm.results for msm in msms]):
                    # if any of the measurements in this prefix events has results
                    self.pfx_events_with_tr_cnt += 1
            except KeyError:
                logging.warning("unable to check pfx event measurements availability: {}".format(
                    pfx_event.as_dict(incl_paths=False)["traceroutes"]
                ))
            for tag in pfx_event.tags:
                tag = tag.name
                count = tag_cnt_dict.get(tag, 0)
                tag_cnt_dict[tag] = count + 1
        self.per_tag_cnt = []
        for tag, cnt in tag_cnt_dict.items():
            self.per_tag_cnt.append({"name": tag, "count": cnt})


