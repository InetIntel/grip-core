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
import json


class ViewMetrics:
    """metrics collected for a whole view"""

    def __init__(self,
                 view_ts, event_type, proc_finished_ts=None, proc_duration=None,
                 consumer_file_path=None,
                 consumer_events_cnt=0, consumer_new_events_cnt=0, consumer_fin_events_cnt=0, consumer_skip_events_cnt=0,
                 consumer_recur_events_cnt=0
                 ):
        # timestamps
        self.view_ts = view_ts
        self.event_type = event_type
        self.proc_finished_ts = proc_finished_ts
        self.proc_duration = proc_duration  # processing delay

        # about consumer data
        self.consumer_file_path = consumer_file_path
        self.consumer_events_cnt = consumer_events_cnt
        self.consumer_new_events_cnt = consumer_new_events_cnt
        self.consumer_fin_events_cnt = consumer_fin_events_cnt
        self.consumer_skip_events_cnt = consumer_skip_events_cnt
        self.consumer_recur_events_cnt = consumer_recur_events_cnt

    def update_proc_time(self, start_ts, current_ts):
        assert(isinstance(start_ts, float) and isinstance(current_ts, float))
        self.proc_finished_ts = int(current_ts)
        self.proc_duration = current_ts - start_ts

    def as_dict(self):
        return {
            # timestamps
            "view_ts": self.view_ts,
            "event_type": self.event_type,
            "proc_duration": self.proc_duration,
            "proc_finished_ts": self.proc_finished_ts,
            # consumer data information
            "consumer_file_path": self.consumer_file_path,
            "consumer_events_cnt": self.consumer_events_cnt,
            "consumer_new_events_cnt": self.consumer_new_events_cnt,
            "consumer_fin_events_cnt": self.consumer_fin_events_cnt,
            "consumer_recur_events_cnt": self.consumer_recur_events_cnt,
            "consumer_skip_events_cnt": self.consumer_skip_events_cnt,
        }

    def get_view_metrics_id(self):
        return "view-metrics-{}-{}".format(self.event_type, self.view_ts)

    @staticmethod
    def from_dict(d):
        return ViewMetrics(**d)

    def as_json_str(self):
        return json.dumps(self.as_dict())
