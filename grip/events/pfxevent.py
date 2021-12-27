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

from grip.active.ripe_atlas.ripe_atlas_msm import AtlasMeasurement
from grip.inference.inference import Inference
from grip.tagger.tags import tagshelper
from grip.tagger.tags.tag import Tag
from .details import PfxEventDetails
from .details_defcon import DefconDetails
from .details_edges import EdgesDetails
from .details_moas import MoasDetails
from .details_submoas import SubmoasDetails

DETAILS_DICT = {
    "moas": MoasDetails,
    "submoas": SubmoasDetails,
    "defcon": DefconDetails,
    "edges": EdgesDetails,
}


class PfxEvent:
    """
    Prefix-event class.
    """

    def __init__(
            self,
            # basic info, also contained in event object
            event_type,
            view_ts,
            position,
            # nullable info, might not have value at the beginning of the pipeline
            finished_ts=None,
            details=None,
            traceroutes=None,
            inferences=None,
            tags=None,
            extra=None,
    ):

        # None-able items
        if traceroutes is None:
            traceroutes = {
                "worthy": False,
                "worthy_tags": [],
                "msms": [],
            }
        if extra is None:
            extra = {}
        if tags is None:
            tags = set()
        if inferences is None:
            inferences = set()

        # convert from dict to objects if necessary
        if isinstance(details, dict):
            details = DETAILS_DICT[event_type].from_dict(details)

        # convert tags in string format to Tag object
        if any(not isinstance(tag, Tag) for tag in tags):
            tags = {tagshelper.parse_tag(tag) for tag in tags}
        # convert tags from list to set
        if isinstance(tags, list):
            tags = set(tags)
        tags = {t for t in tags if t}  # remove none tags

        # parse measurements
        if any(isinstance(msm, dict) for msm in traceroutes["msms"]):
            traceroutes["msms"] = [AtlasMeasurement.from_dict(msm) for msm in traceroutes["msms"]]

        # parse inferences
        if any(isinstance(inference, dict) for inference in inferences):
            # convert list of inference from dicts to set of Inference objects
            inferences = {Inference.from_dict(inference) for inference in inferences}

        # sanity checks
        assert event_type in ["moas", "submoas", "defcon", "edges"]
        assert isinstance(view_ts, int)
        assert isinstance(details, PfxEventDetails)
        assert isinstance(extra, dict)
        assert isinstance(tags, set)
        assert all([isinstance(item, AtlasMeasurement) for item in traceroutes["msms"]])

        self.event_type = event_type
        self.position = position
        self.view_ts = int(view_ts)
        self.finished_ts = finished_ts
        self.details = details
        self.traceroutes = traceroutes
        self.extra = extra
        self.tags = tags
        self.inferences = set(inferences)

    def __repr__(self):
        return json.dumps(self.as_dict())

    def get_event_id(self):
        """
        Extract the event ID instead of the prefix event ID. This is used for grouping prefix events to corresponding events.
        """
        return "{}-{}-{}".format(self.event_type, self.view_ts, self.details.get_origin_fingerprint())

    def get_recurring_fingerprint(self):
        """
        Extract fingerprint of the prefix event for detecting recurring prefix events. All prefix events that has the
        same fingerprint within the past window (e.g. 24 hours) will be considered recurring.
        :return: fingerprint in string format
        """

        if self.event_type == "edges":
            return self.details.get_origin_fingerprint()
        else:
            return "{}-{}".format(
                self.details.get_origin_fingerprint(),
                self.details.get_prefix_fingerprint())

    def as_dict(self, incl_paths=True):
        """
        Convert current PfxEvent object into a dict
        """
        attackers, victims = self.details.extract_attackers_victims()

        d = {
            "event_type": self.event_type,
            "view_ts": self.view_ts,
            "finished_ts": self.finished_ts,
            "position": self.position,
            "details": self.details.as_dict(incl_paths),
            "traceroutes": {
                "worthy": self.traceroutes["worthy"],
                "worthy_tags": self.traceroutes["worthy_tags"],
                "msms": [msm.as_dict() for msm in self.traceroutes["msms"]],
            },
            "attackers": list(attackers),
            "victims": list(victims),
            "tags": [t.as_dict() for t in self.tags],
            "inferences": [i.as_dict() for i in self.inferences],
            "extra": self.extra,
        }
        return d

    @staticmethod
    def from_dict(d):
        """
        Extract a PfxEvent object from a dict
        """
        # remove attackers and victims (extracted on the fly when exporting)
        d.pop("attackers", None)
        d.pop("victims", None)

        return PfxEvent(**d)

    def add_tags(self, tags):
        """
        Add a set (or list) of tags into this prefix event. The tag can be either in the form of string or Tag object.
        """
        assert isinstance(tags, (list, set))
        # convert tags to Tag objects
        if any(not isinstance(tag, Tag) for tag in tags):
            tags = {tagshelper.parse_tag(tag) for tag in tags}
        self.tags.update(tags)

    def has_tag(self, tag):
        """
        Check whether the prefix event has a given tag
        :param tag: a tag represented by either string or a Tag object
        :return: True if prefix event has the given tag
        """
        if isinstance(tag, str):
            tag = tagshelper.get_tag(tag)
        return tag.name in [t.name for t in self.tags]

    def add_inferences(self, inferences):
        """
        Add the given list/set of inferences into the current prefix event
        """
        if not inferences:
            return
        self.inferences.update(set(inferences))

    def remove_inferences(self, inferences):
        """
        Add the given set of inferences into the current prefix event
        """
        if not inferences:
            return
        self.inferences -= set(inferences)
