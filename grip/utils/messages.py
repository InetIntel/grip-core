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

from grip.active.ripe_atlas.ripe_atlas_msm import AtlasMeasurement


class Message:
    def __init__(self, sender):
        self.sender = sender


class EventOnElasticMsg(Message):
    """
    Message indicating event ready message to be retrieved from ElasticSearch.
    """

    def __init__(self, sender, es_index, es_id, tr_worthy, es_doc_type="event_result", process_finished=False):
        Message.__init__(self, sender)
        self.es_index = es_index
        self.es_id = es_id
        self.es_doc_type = es_doc_type
        self.tr_worthy = tr_worthy
        self.process_finished = process_finished

    def to_str(self):
        return " ".join(
            [self.sender, self.es_index, self.es_id, self.es_doc_type, str(self.tr_worthy), str(self.process_finished)])

    def to_url(self):
        return "http://clayface.caida.org:9200/{}/{}/{}".format(
            self.es_index, self.es_doc_type, self.es_id
        )

    @staticmethod
    def from_str(value):
        fields = value.split(" ")
        # sender, es_index, es_id, es_doc_type, tr_worthy = value.split(" ")
        sender = fields[0]
        es_index = fields[1]
        es_id = fields[2]
        es_doc_type = fields[3]
        tr_worthy = fields[4] == "True"
        process_finished = True
        if len(fields) == 6:
            process_finished = fields[5] == "True"
        return EventOnElasticMsg(
            sender=sender, es_index=es_index, es_id=es_id, es_doc_type=es_doc_type,
            tr_worthy=tr_worthy, process_finished=process_finished)


class MeasurementsRequestedMsg(Message):
    """
    Message indicating active probing measurements has been requested.
    """

    def __init__(self, sender, event_type, view_ts, event_id, measurements):
        Message.__init__(self, sender)
        self.event_type = event_type
        self.view_ts = str(view_ts)
        self.event_id = event_id
        self.measurements = measurements

    def to_str(self):
        return " ".join(
            [
                self.sender,
                self.event_type,
                self.view_ts,
                self.event_id,
                ";".join([msm.as_str() for msm in self.measurements])
            ]
        )

    @staticmethod
    def from_event(sender, event_type, view_ts, event_id, measurements):
        return MeasurementsRequestedMsg(
            sender=sender,
            event_type=event_type,
            view_ts=view_ts,
            event_id=event_id,
            measurements=measurements,
        )

    @staticmethod
    def from_str(value):
        fields = value.split(" ")
        msms_str = " ".join(fields[4:])
        msms = [AtlasMeasurement.from_str(msm_str) for msm_str in msms_str.split(";")]
        return MeasurementsRequestedMsg(
            sender=fields[0],
            event_type=fields[1],
            view_ts=fields[2],
            event_id=fields[3],
            measurements=msms,
        )
