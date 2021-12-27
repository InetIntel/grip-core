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
import hashlib
import json


class Inference:
    """
    FIXME: An Inference takes information from a given prefix event and produce meta information about the event,
    such as the suspicion level of the prefix event, whether it's part of a misconfiguration, etc.

    An Inference has the following fields:
    - inference_id: an unique identifier for the inference
    - explanation: the description of the inference in string (not intended for user consumption)
    - suspicion_level: (optional) an integer between 0-100 representing how suspicious the event is
    - confidence: (optional) how confidence our system is about the inference (or suspicion_level -- FIXME: we still have to decide)
    - labels: (optional) labels that can be used for grouping inferences (e.g., by category), represented as a list of strings
    - pfx_event_ids: (optional) list of IDs of prefix events that have this inference

    Each Inference is identifiable by its "inference_id".

    The inferences is sortable by two levels:
    1. sort by confidence, higher confidence means higher ranking
    2. sort by suspicion_level, higher suspicion_level means higher ranking
    FIXME: decide whether to allow inference with no confidence or suspicion level

    For example, for (confidence, suspicion_level) pairs:
    (90, 20) > (80,30) # higher confidence
    (90, 20) > (90,10) # same confidence, higher suspicion_level
    """

    def __init__(self,
                 inference_id, explanation="",
                 suspicion_level=-1, confidence=-1, labels=None):
        self.inference_id = inference_id
        self.explanation = explanation
        self.suspicion_level = suspicion_level
        self.confidence = confidence
        if labels is None:
            labels = list()
        self.labels = labels

    def __lt__(self, other):
        """
        Determine whether current inference is less than the other inference passed as parameter.

        There are two cases that the current inference is less than the other:
        1. the confidence level is less
        2. same confidence but has lower suspicion_level
        """
        assert isinstance(other, Inference)
        if not self.confidence:
            # if itself doesn't have a confidence level, it's always less than the other
            return True

        less_than = False

        if self.confidence < other.confidence:
            # lower confidence -> lower ranking
            less_than = True
        elif self.confidence == other.confidence:
            # both have the same confidence level, now compare the suspicion levels
            # lower suspicion_level -> lower ranking
            less_than = self.suspicion_level < other.suspicion_level

        return less_than

    def __hash__(self):
        """
        Two inferences are the same if they have the same inference_id and confidence level
        """
        hash_str = "{}.{}".format(self.inference_id, self.confidence)
        return int(hashlib.md5(hash_str.encode()).hexdigest()[:8], 16)

    def __repr__(self):
        return self.as_json_str()

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        return (
            isinstance(other, Inference) and
            self.inference_id == other.inference_id and
            self.confidence == other.confidence
        )

    def as_dict(self):
        """return as dict"""
        return {
            "inference_id": self.inference_id,
            "explanation": self.explanation,
            "suspicion_level": self.suspicion_level,
            "confidence": self.confidence,
            "labels": self.labels,
        }

    @staticmethod
    def from_dict(d):
        """Parse a dict into a Inference object"""
        return Inference(**d)

    def as_json_str(self):
        """return as single-line JSON string"""
        return json.dumps(self.as_dict())
