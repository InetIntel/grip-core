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

import grip.inference
from grip.tagger.tags.tag import Tag


class EventSummary:
    """
    Summary of the event. All information in the summary is extracted from the information of the
    prefix events. Given a list of prefix events, we can generate this summary.

    Fields include:
    - prefixes
    - ases
    - tr_worthy
    - newcomers
    - tags: names of the tags, values and other properties are ignored for briviety. see pfx event for detailed tags
    - inference_result (inferences, primary_inference)
    """

    def __init__(self, event):
        # NOTE: _event will not be exported
        self._event = event

        self.prefixes = set()
        self.ases = set()
        self.newcomers = set()
        self.tags = set()
        self.tr_worthy = False
        self.inference_result = None
        self.attackers = set()
        self.victims = set()

    def clear_inference(self):
        self.inference_result = None
        for pfx_event in self._event.pfx_events:
            pfx_event.inferences = set()

    def as_dict(self):
        return {
            "prefixes": list(self.prefixes),
            "ases": list(self.ases),
            "newcomers": list(self.newcomers),
            "tags": [t.as_dict() for t in self.tags if t],
            "tr_worthy": self.tr_worthy,
            "inference_result": self.inference_result.as_dict(),
            "attackers": list(self.attackers),
            "victims": list(self.victims),
        }

    def update(self):
        """
        Generate a summary for the event
        """
        assert self._event is not None

        inference_set = set()
        for pfx_event in self._event.pfx_events:
            self.ases.update(pfx_event.details.get_current_origins())
            self.tags.update(pfx_event.tags)
            self.newcomers.update(pfx_event.details.get_new_origins())
            self.prefixes.update(pfx_event.details.get_prefixes())
            self.tr_worthy = self.tr_worthy | pfx_event.traceroutes["worthy"]
            inference_set.update(pfx_event.inferences)

        self.attackers, self.victims = self._extract_attackers_victims()
        self.inference_result = grip.inference.InferenceResult(inferences=inference_set)

    def has_tag(self, tag: Tag, match_name_only=True):
        """
        check if summary has certain tag
        """
        if match_name_only:
            return tag.name in {t.name for t in self.tags}
        else:
            return tag in self.tags

    def _extract_attackers_victims(self):
        """
        Infer the attackers and victims for an Event
        :return:
        """

        attackers = set()
        victims = set()

        for pfx_event in self._event.pfx_events:
            a, v = pfx_event.details.extract_attackers_victims()
            attackers.update(a)
            victims.update(v)
        return attackers, victims
