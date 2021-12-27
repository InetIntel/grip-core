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
import grip.events.event
import grip.events.pfxevent
from grip.events.details_submoas import SubmoasDetails
from .inference import Inference

# define labels to use here to avoid misspell labels for inferences
# TODO: reassign labels for proper search capabilities
LABEL_MISCONFIG = "misconfig"
LABEL_FATFINGER = "fatfinger"
LABEL_COMMENT = "comment"
LABEL_ROUTE_LEAK = "route-leak"
LABEL_HIDE = "hide"
LABEL_SUSPICIOUS = "suspicious"
LABEL_GREY = "grey"
LABEL_BENIGN = "benign"
LABEL_TRACEROUTE = "traceroute"

DEFAULT_WORTHY_INFERENCE = Inference(
    inference_id="default-tr-worthy",
    explanation="no other inferences found, event is traceroute worthy",
    suspicion_level=80,
    confidence=50,
    labels=[LABEL_TRACEROUTE]
)

DEFAULT_NOT_WORTHY_INFERENCE = Inference(
    inference_id="default-not-tr-worthy",
    explanation="no other inferences found, event is not traceroute worthy",
    suspicion_level=20,
    confidence=50,
    labels=[LABEL_TRACEROUTE]
)


class InferenceEngine:
    """
    The InferenceEngine contains logic that processes information from a prefix event and produces inferences.
    """

    def __init__(self):
        pass

    def infer_on_event(self, event):
        """
        Conduct inference on a event
        :param event:
        :return:
        """
        assert isinstance(event, grip.events.event.Event)

        # infer on each prefix event
        for pfx_event in event.pfx_events:
            assert isinstance(pfx_event, grip.events.pfxevent.PfxEvent)

            ####
            # High-level inference methodology logic starts here
            ####

            # The taggers tag only MAX_PFX_EVENTS_PER_EVENT_TO_TAG prefix-events per event
            if pfx_event.has_tag("skipped-pfx-event"):
                continue

            # Prefix events to be discarded
            inferences = self._infer_discard_events(pfx_event)
            if inferences:
                pfx_event.add_inferences(inferences)
                continue

            # Bugs/Unexpected characteristics
            inferences = self._infer_bug_events(pfx_event)
            if inferences:
                pfx_event.add_inferences(inferences)
                continue

            ####
            # 1. Very obvious benign cases. If inference matches don't go further.
            ####

            # Caused by a private ASN and/or AS23456
            # We could hide these events but decided to still show them in case people want to search for them (e.g.,
            # for troubleshooting purposes)
            if pfx_event.event_type in ["submoas", "moas"]:
                inferences = self._infer_private_asn(pfx_event)
                if inferences:
                    pfx_event.add_inferences(inferences)
                    continue

            # Siblings
            if pfx_event.event_type in ["submoas", "moas"]:
                inferences = self._infer_siblings(pfx_event)
                if inferences:
                    pfx_event.add_inferences(inferences)
                    continue

            # Reserved space
            inferences = self._infer_reserved_space(pfx_event)
            if inferences:
                pfx_event.add_inferences(inferences)
                continue

            ####
            # 2. Still very likely benign, but we will also check for further inferences
            ####

            # MOAS transition events
            if pfx_event.event_type in ["moas"]:
                pfx_event.add_inferences(self._infer_moas_transition(pfx_event))

            # Some newcomers are siblings. TODO: check if we ever see this
            if pfx_event.event_type in ["submoas", "moas"]:
                pfx_event.add_inferences(self._infer_partial_siblings(pfx_event))

            # Caused by a DPS ASN
            if pfx_event.event_type in ["submoas", "moas"]:
                pfx_event.add_inferences(self._infer_dps_asn(pfx_event))

            # SubMOAS less specific
            if pfx_event.event_type in ["submoas"]:
                pfx_event.add_inferences(self._infer_less_specific(pfx_event))

            # MOAS/SubMOAS all newcomers
            if pfx_event.event_type in ["submoas", "moas"]:
                pfx_event.add_inferences(self._infer_all_newcomers(pfx_event))

            # SubMOAS where both prefixes appeared within the 5-min bin.
            # This is covered by all-newcomers tag

            ####
            # FIXME likely benign/misconf but check for blacklisted ASN
            ####

            # Check for more specific than /24 (probably blackholing)
            pfx_event.add_inferences(self._infer_very_specific(pfx_event))

            # TODO: a prefix less specific than a /8 is likely a misconfiguration. But if it's coming from
            #  a blacklisted ASN we want to infer it as more suspicious. There might be more classes
            #  of events like this for which we want to check for blacklisting but then, independently
            #  of the result, continue to the next event

            # TODO: Check if the prefix is owned by an IXP and the two origins are co-located at an (any) IXP
            # TODO: pfx_event.add_inferences(check_ixp_prefix_and_colo(pfx_event))

            ####
            # 4. Other cases
            ####

        # start another loop to make sure all prefix events have at least a default inference
        for pfx_event in event.pfx_events:
            # if no inferences were generated, add default inference
            if not pfx_event.inferences:
                pfx_event.add_inferences([self._get_default_inference(pfx_event.traceroutes["worthy"])])
            if len(pfx_event.inferences) > 1:
                # if there are more than one inferences, remove the default tr worthiness inference
                pfx_event.remove_inferences({DEFAULT_WORTHY_INFERENCE, DEFAULT_NOT_WORTHY_INFERENCE})

        event.summary.update()

    #######################
    #######################
    #######################
    # Inference Functions #
    #######################
    #######################
    #######################

    #####################
    # To Discard Events #
    #####################

    @staticmethod
    def _infer_discard_events(pfx_event):
        """
        Check and decide whether to discard the event
        """

        inferences = []

        if pfx_event.has_tag("recurring-pfx-event"):
            inferences.append(
                Inference(
                    inference_id="hide-recurring-pfx-event",
                    explanation="recurring event; hide event",
                    suspicion_level=-1,
                    confidence=100,
                    labels=[LABEL_HIDE]
                )
            )

        if pfx_event.has_tag("short-prefix"):
            inferences.append(
                Inference(
                    inference_id="hide-short-prefix",
                    explanation="prefix is too large (less specific than /8); hide event",
                    suspicion_level=-1,
                    confidence=100,
                    labels=[LABEL_HIDE]
                )
            )

        #        if pfx_event.has_tag("subpfx-moas") or pfx_event.has_tag("superpfx-moas"):
        #            inferences.append(
        #                Inference(
        #                    inference_id="hide-moas-caused-submoas",
        #                    explanation="a moas event that also causes a submoas; hide",
        #                    suspicion_level=-1,
        #                    confidence=100,
        #                    labels=[LABEL_HIDE]
        #                )
        #            )

        if pfx_event.has_tag("submoas-covered-by-moas-subpfx") or \
                pfx_event.has_tag("submoas-covered-by-moas-superpfx"):
            inferences.append(
                Inference(
                    inference_id="hide-submoas-covered-by-moas",
                    explanation="This is a MOAS in the first place",
                    suspicion_level=-1,
                    confidence=100,
                    labels=[LABEL_HIDE]
                )
            )

        if pfx_event.has_tag("no-newcomer") and pfx_event.has_tag("less-origins"):
            inferences.append(
                Inference(
                    inference_id="hide-shrinking-event",
                    explanation="a shrinking moas event; hide event",
                    suspicion_level=-1,
                    confidence=100,
                    labels=[LABEL_HIDE]
                )
            )

        return inferences

    @staticmethod
    def _infer_bug_events(pfx_event):
        """
        Check and decide whether the event is likely affected by a bug or is an artifact due to rebooting, etc.
        """

        inferences = []

        # FIXME: decide if we should consider edges and/or defcon too
        if pfx_event.event_type in ["submoas", "moas"]:
            if pfx_event.has_tag("no-newcomer") and not pfx_event.has_tag("less-origins"):
                if pfx_event.has_tag("outdated-info"):
                    inferences.append(
                        Inference(
                            inference_id="redis-outdated-causes-no-newcomer",
                            explanation="There are no-newcomers, likely because we retrieved outdated info from the "
                                        "redis db",
                            suspicion_level=-1,
                            confidence=100,
                            labels=[LABEL_HIDE]
                        )
                    )
                else:
                    inferences.append(
                        Inference(
                            inference_id="likely-reboot-causes-no-newcomer",
                            explanation="There are no-newcomers, likely because we restarted the consumers",
                            suspicion_level=-1,
                            confidence=100,
                            labels=[LABEL_HIDE]
                        )
                    )
        return inferences

    ####################
    # Obviously Benign #
    ####################

    @staticmethod
    def _infer_private_asn(pfx_event):
        """
        Inference based on whether the event is caused by a private ASN and/or AS 23456
        """

        inferences = []

        if pfx_event.has_tag("due-to-private-asn"):
            inferences.append(
                Inference(
                    inference_id="due-to-private-asn",
                    explanation="The event is caused by private ASNs",
                    suspicion_level=1,
                    confidence=99,
                    labels=[LABEL_BENIGN]
                )
            )

        if pfx_event.has_tag("due-to-as-trans"):
            inferences.append(
                Inference(
                    inference_id="due-to-as-trans",
                    explanation="The event is caused by AS23456 - Probably an artifact. We currently don't"
                                "support translating this ASN",
                    suspicion_level=1,
                    confidence=99,
                    labels=[LABEL_BENIGN]
                )
            )

        if pfx_event.has_tag("due-to-private-and-as-trans"):
            inferences.append(
                Inference(
                    inference_id="due-to-private-and-as-trans",
                    explanation="The event is caused by a combination of private ASNs and AS23456 - Probably an "
                                "artifact. We currently don't support translating this ASN",
                    suspicion_level=1,
                    confidence=99,
                    labels=[LABEL_BENIGN]
                )
            )

        return inferences

    @staticmethod
    def _infer_reserved_space(pfx_event):
        """
        :param pfx_event: PfxEvent object
        :return:
        """

        inferences = []

        if pfx_event.has_tag("reserved-space"):
            inferences.append(
                Inference(
                    inference_id="misconfig-private-prefix",
                    explanation="Reserved prefix announcements cannot (should not) attract traffic, "
                                "likely to be caused by misconfiguration",
                    suspicion_level=2,
                    confidence=99,
                    labels=[LABEL_MISCONFIG]
                )
            )

        return inferences

    @staticmethod
    def _infer_siblings(pfx_event):
        """
        :param pfx_event: PfxEvent object
        :return:
        """

        inferences = []

        if pfx_event.has_tag("newcomer-all-siblings") or pfx_event.has_tag("all-siblings"):
            inferences.append(
                Inference(
                    inference_id="sibling-origins",
                    explanation="All ASes involved are siblings.",
                    confidence=99,
                    suspicion_level=1,
                    labels=[LABEL_BENIGN]
                )
            )

        return inferences

    #################
    # Likely Benign #
    #################

    @staticmethod
    def _infer_moas_transition(pfx_event):
        """
        :param pfx_event: PfxEvent object
        :return:
        """
        inferences = []

        if pfx_event.has_tag("moas-transition"):
            if pfx_event.has_tag("moas-potential-transfer"):
                inferences.append(
                    Inference(
                        inference_id="moas-potential-transfer",
                        explanation="MOAS is potentially caused by prefix ownership transfer (transition A to A,"
                                    "B to B lasting more than 5 minutes)",
                        suspicion_level=20,
                        confidence=90,
                        labels=[LABEL_BENIGN]
                    )
                )
            elif pfx_event.has_tag("moas-potential-convergence"):
                inferences.append(
                    Inference(
                        inference_id="moas-potential-convergence",
                        explanation="MOAS is potentially due to bgp convergence (transition A to A,B to B lasting at "
                                    "most 5 minutes)",
                        suspicion_level=20,
                        confidence=90,
                        labels=[LABEL_BENIGN]
                    )
                )

        return inferences

    @staticmethod
    def _infer_partial_siblings(pfx_event):
        """
        :param pfx_event: PfxEvent object
        :return:
        """
        inferences = []

        if pfx_event.has_tag("newcomer-some-siblings") and not pfx_event.has_tag("newcomer-all-siblings"):
            inferences.append(
                Inference(
                    inference_id="some-sibling-origins",
                    explanation="At least one AS (but not all) who just started announcing this prefix "
                                "is a sibling of one of the pre-existing origin(s)",
                    confidence=90,
                    suspicion_level=2,
                    labels=[LABEL_BENIGN]
                )
            )

        return inferences

    @staticmethod
    def _infer_dps_asn(pfx_event):
        """
        Inference based on whether the event is caused by a DPS (DoS protection) ASN
        """
        inferences = []

        if pfx_event.has_tag("due-to-dps-asn"):
            inferences.append(
                Inference(
                    inference_id="dps-asn",
                    explanation="The event is caused by ASNs performing DoS Protection Services (DPS)",
                    suspicion_level=10,
                    confidence=60,  # low confidence because we're gradually enlarging this whitelist
                    labels=[LABEL_BENIGN]
                )
            )

        return inferences

    @staticmethod
    def _infer_less_specific(pfx_event):
        """
        Inference based on whether the SubMOAS is due to a new less specific prefix
        """
        inferences = []

        if pfx_event.has_tag("newcomer-less-specific"):
            inferences.append(
                Inference(
                    inference_id="newcomer-less-specific",
                    explanation="less specific prefix cannot attract traffic",
                    suspicion_level=10,
                    confidence=95,
                    labels=[LABEL_BENIGN]
                )
            )

        return inferences

    @staticmethod
    def _infer_all_newcomers(pfx_event):
        """
        Check for a MOAS/SubMOAS event with all newcomers.
        """
        inferences = []

        if pfx_event.has_tag("all-newcomers"):
            inferences.append(
                Inference(
                    inference_id="all-newcomers",
                    explanation="all origins are newcomers: 1) MOAS: the prefix was not announced"
                                " before or the oldcomer disappeared and (at least) 2 new origins appeared at the "
                                "same time; 2) subMOAS: similarly, all origins responsible for the subMOAS are "
                                "newcomers",
                    suspicion_level=3,
                    confidence=90,
                    labels=[LABEL_BENIGN]
                    # There is still a chance that the events happened separately and independently within the 5 min
                    # bin. While it is unlikely that a malicious actor waits for the announcement of a prefix to
                    # automatically hijack it, it's possible that instead this was an automated remediation to a hijack
                    # E.g., A owns and normally announces a /16, B (attacker) announces a /20, A reacts announcing a
                    # /21 -> We see a submoas event for B/20 and A/21 happening within a 5 min bin. We would mark this
                    # remediation as benign, which is fine, but we would still observe a second submoas event (A/16
                    # B/20), which doesn't have the all-newcomers tag, and would be the one correctly used to
                    # evaluate the attack.
                    # There is another possible case: when the attacker was squatting space (i.e., A does not
                    # announce an overlapping less specific prefix) but the victim was monitoring the space and reacts
                    # automatically. In this case there wasn't a submoas in the first place and there is only this
                    # submoas, which we mark as benign (this is indeed just the remediation, so it's fine). Catching
                    # the squatting would require a different type of event (i.e., squatting is not a submoas).
                    # TODO: Finally, as more general consideration, outside of this "all-newcomers" case, what happens
                    #  when there is a remediation outside of the same time bin? We see a submoas between B/20 and A/21
                    #  and we might consider A the potential attacker (when it's instead just the victim reacting).
                    #  We could recognize this case by checking if A originates a less specific than the /21 and
                    #  assigning it a tag "newcomer-originated-less-specific", which we might use to infer that this
                    #  is a potential remediation action. However, this would create an opportunity for an attacker
                    #  to first originate a less specific as a sort of shield to trigger our wrong benign inference
                    #  later:
                    #  e.g., t0:A/16; t1:A/16,B/15 (inferred as benign submoas newcomer-less-specific); t3:A/16,B/15,
                    #  B/24 (we would infer A/16 B/24 as benign just because B was originating a /15 too).
                )
            )

        return inferences

    ###########################################
    # Likely Benign But Check Blacklist First #
    ###########################################

    @staticmethod
    def _infer_very_specific(pfx_event):
        """
        :param pfx_event: PfxEvent object
        :return:
        """
        inferences = []
        if pfx_event.has_tag("single-ip"):
            inferences.append(
                Inference(
                    inference_id="blackholed-address",
                    explanation="/32 prefix is probably blackholed (e.g., for DoS mitigation)",
                    confidence=90,
                    suspicion_level=5,
                    labels=[LABEL_BENIGN]
                )
            )
            return inferences

        if pfx_event.has_tag("long-prefix"):
            inferences.append(
                Inference(
                    inference_id="blackholed-prefix",
                    explanation="Prefix more specific than a /24 (unlikely to be propagated). "
                                "It could be blackholing (e.g., for DoS mitigation)",
                    confidence=90,
                    suspicion_level=9,
                    labels=[LABEL_BENIGN]
                )
            )

        return inferences

    ##########################
    # Default Catch-All Case #
    ##########################

    @staticmethod
    def _get_default_inference(tr_worthy):
        if tr_worthy:
            return DEFAULT_WORTHY_INFERENCE
        else:
            return DEFAULT_NOT_WORTHY_INFERENCE

    ############################
    ############################
    ############################
    # BELOW TO BE REVISED/USED #
    ############################
    ############################
    ############################

    @staticmethod
    def _infer_suspicious_asns(pfx_event):
        """
        Infer based on whether origins of the prefix event are on black lists
        """

        inferences = []

        if pfx_event.has_tag("blacklist-asn"):
            inferences.append(
                Inference(
                    inference_id="suspicious-blacklist-asn",
                    explanation="the AS was reported for malicious behaviors before",
                    suspicion_level=70,
                    confidence=60,
                    labels=[LABEL_SUSPICIOUS]
                )
            )
        if pfx_event.has_tag("spamhaus-asn-drop"):
            inferences.append(
                Inference(
                    inference_id="suspicious-spamhaus-asn",
                    explanation="the AS was on spamhaus asn-drop list",
                    suspicion_level=80,
                    confidence=80,
                    labels=[LABEL_SUSPICIOUS]
                )
            )

        return inferences

    @staticmethod
    def _infer_edges(pfx_event):

        """
        Check new edge cases
        """
        if pfx_event.event_type not in ["edges"]:
            return []

        inferences = []

        if not pfx_event.has_tag("new-bidirectional") and \
           not pfx_event.has_tag("adj-previously-observed-exact") and \
           not pfx_event.has_tag("adj-previously-observed-opposite"):
            inferences.append(
                Inference(
                    inference_id="new-one-direction-edge",
                    explanation="This new edge has never been observed in any direction in the past and has "
                                "it has just appeared only in 1 direction (this is a potential new link or a "
                                "suspicious event)",
                    suspicion_level=20,
                    confidence=80,
                    labels=[]
                )
            )

        return inferences

    @staticmethod
    def _infer_misconfig(pfx_event):
        """
        Check the event for potential misconfiguration
        """

        inferences = []

        if pfx_event.has_tag("all-newcomers-next-to-an-oldcomer") \
                and pfx_event.has_tag("newcomer-small-asn"):
            inferences.append(
                Inference(
                    inference_id="misconfig-fatfinger-prepend",
                    explanation="newcomer is very small ASN, likely to be prepending misconfiguration",
                    suspicion_level=20,
                    confidence=90,
                    labels=[LABEL_MISCONFIG, LABEL_FATFINGER]
                )
            )

        if pfx_event.has_tag("origin-small-edit-distance"):
            if pfx_event.has_tag("newcomer-some-siblings"):
                # This prevents to mark it as misconfiguration when the small edit distance is just due to the
                # fact that the newcomer is sibling of the oldcomer (e.g., they got assigned consecutive ASNs)
                # We need to catch this case because we don't stop the inference earlier if "newcomer-some-siblings"
                # is set (we do stop earlier if "newcomer-all-siblings" is set).
                # NOTE: Actually we don't strictly check that the newcomer that caused the small edit distance
                # is the same that is sibling with the oldcomer. But still if one of the newcomer is sibling
                # it is likely that this was an operation from a sibling and thus the reasoning above still applies
                inferences.append(
                    Inference(
                        inference_id="sibling-close-asn",
                        explanation="The ASNs of (at least one) newcomer AS and (at least one) oldcomer AS are "
                                    "siblings/friends and also have small edit distance. "
                                    "This event is likely benign and probably not a misconfiguration.",
                        confidence=91,  # This value will give this inference priority to the newcomer-some-siblings
                        suspicion_level=2,
                        labels=[LABEL_BENIGN]
                    )
                )
            else:
                inferences.append(
                    Inference(
                        inference_id="misconfig-fatfinger-asn",
                        explanation="The ASNs of the potential attacker and the potential victim are "
                                    "siblings/friends and also have small edit distance.",
                        suspicion_level=20,
                        confidence=50,
                        labels=[LABEL_MISCONFIG, LABEL_FATFINGER]
                    )
                )

        if pfx_event.has_tag("prefix-small-edit-distance"):
            if pfx_event.has_tag("newcomer-some-siblings"):
                # same reasoning as above. it's not a misconfiguration.
                inferences.append(
                    Inference(
                        inference_id="sibling-close-prefix",
                        explanation="The ASNs of (at least one) newcomer AS and (at least one) oldcomer AS are "
                                    "siblings/friends and previously announced similar prefixes. "
                                    "This event is likely benign and probably not a misconfiguration.",
                        confidence=91,  # This value will give this inference priority to the newcomer-some-siblings
                        suspicion_level=2,
                        labels=[LABEL_BENIGN]
                    )
                )
            else:
                inferences.append(
                    Inference(
                        inference_id="misconfig-fatfinger-prefix",
                        explanation="The ASNs of the potential attacker previously announced similar prefix, "
                                    "this is more likely to be caused by a misconfiguration",
                        suspicion_level=20,
                        confidence=50,
                        labels=[LABEL_MISCONFIG, LABEL_FATFINGER]
                    )
                )

        return inferences

    @staticmethod
    def _infer_potential_pollution(pfx_event):
        """
        Check if the prefix event is caused by pollution.

        Confidence calculation based on prefix size:
        anything below 8 is confidence 95
        8 -> 90
        9 -> 80
        10 -> 70
        11 -> 60
        12 -> 50
        13 -> 40
        14 -> 30
        15 -> 20
        16 -> 10
        17+ -> POTENTIAL NORMAL
        """

        if pfx_event.event_type not in ["submoas"]:
            return []

        inferences = []

        if pfx_event.has_tag("newcomer-less-specific"):

            details = pfx_event.details
            assert isinstance(details, SubmoasDetails)
            super_pfx_size = int(details.get_super_pfx().split("/")[1])

            if super_pfx_size < 8:
                confidence = 95
            else:
                confidence = 90 - (super_pfx_size - 8) * 10

            # it's possible that the super-prefix size is smaller than 16, which
            # would result in confidence smaller or equals to 0.
            if confidence > 0:
                inferences.append(
                    Inference(
                        inference_id="newcomer-less-specific-potential-pollution",
                        explanation="newcomer super-prefix is less specific than /16 prefix",
                        suspicion_level=20,
                        confidence=confidence,
                        labels=[LABEL_ROUTE_LEAK]
                    )
                )
            else:
                inferences.append(
                    Inference(
                        inference_id="newcomer-less-specific-potential-normal-operation",
                        explanation="newcomer super-prefix is more specific than /16 prefix",
                        suspicion_level=10,
                        confidence=confidence,
                        labels=[LABEL_ROUTE_LEAK]
                    )
                )

        return inferences

    @staticmethod
    def _infer_relationship(pfx_event):
        """
        Inference based on AS relationships of the origins
        """

        inferences = []

        if pfx_event.has_tag("some-newcomers-are-providers"):
            inferences.append(
                Inference(
                    inference_id="some-newcomers-are-providers",
                    explanation="some newcomers are providers of the previous origin",
                    suspicion_level=50,
                    confidence=70,
                    labels=[LABEL_BENIGN]
                )
            )

        if pfx_event.has_tag("all-newcomers-are-peers"):
            inferences.append(
                Inference(
                    inference_id="all-newcomers-are-peers",
                    explanation="all newcomers are peers with some previous origins",
                    suspicion_level=20,
                    confidence=70,
                    labels=[LABEL_BENIGN]
                )
            )

        if pfx_event.has_tag("all-newcomers-are-providers") \
                or pfx_event.has_tag("all-newcomers-are-customers"):
            inferences.append(
                Inference(
                    inference_id="all-newcomers-are-providers",
                    explanation="newcomers are all providers of one of the previous origins",
                    suspicion_level=20,
                    confidence=70,
                    labels=[LABEL_BENIGN]
                )
            )

        if pfx_event.has_tag("all-newcomers-are-rel-upstream") \
                or pfx_event.has_tag("all-newcomers-are-rel-downstream"):
            inferences.append(
                Inference(
                    inference_id="all-newcomers-are-upstreams",
                    explanation="newcomers are all upstream providers",
                    suspicion_level=20,
                    confidence=60,
                    labels=[LABEL_BENIGN]
                )
            )

        if pfx_event.has_tag("single-rel-upstream-chain"):
            inferences.append(
                Inference(
                    inference_id="customer-provider-single-chain",
                    explanation="All origins in the event are in a single customer-provider chain",
                    suspicion_level=20,
                    confidence=90,
                    labels=[LABEL_BENIGN]
                )
            )

        return inferences

    @staticmethod
    def _infer_prefix(pfx_event):
        """
        Inference based on the nature of the prefixes
        """

        inferences = []

        if pfx_event.has_tag("not-previously-announced"):
            inferences.append(
                Inference(
                    inference_id="new-prefix",
                    explanation="the prefix is not previously announced (brand-new prefix)",
                    suspicion_level=20,
                    confidence=80,
                    labels=[LABEL_BENIGN]
                )
            )

        if pfx_event.has_tag("long-prefix"):
            inferences.append(
                Inference(
                    inference_id="long-prefix",
                    explanation="very long prefix (longer than /32) does not attack much traffic and are likely to be triggered by "
                                "normal traffic engineering",
                    suspicion_level=10,
                    confidence=90,
                    labels=[LABEL_BENIGN]
                )
            )

        return inferences

    @staticmethod
    def _infer_ixp_colo_prefix(pfx_event):
        """
        Check for other benign events
        """

        inferences = []

        if pfx_event.has_tag("ixp-colocated") and pfx_event.has_tag("ixp-prefix"):
            inferences.append(
                Inference(
                    inference_id="ixp-prefix-and-colocated",
                    explanation="prefix owned by an IXP and the newcomer origins are colocated in some IXP with old origins",
                    suspicion_level=20,
                    confidence=60,
                    labels=[LABEL_BENIGN]
                )
            )

        return inferences

    @staticmethod
    def _infer_oldcomers_on_paths(pfx_event):
        """
        Inference based on AS paths observed
        """

        inferences = []

        if pfx_event.has_tag("oldcomers-always-on-newcomer-originated-paths") or \
                pfx_event.has_tag("all-newcomers-next-to-an-oldcomer"):
            inferences.append(
                Inference(
                    inference_id="oldcomer-on-paths",
                    explanation="old origins are always on the paths of the new origins, "
                                "traffic flow through old origins first",
                    suspicion_level=20,
                    confidence=80,
                    labels=[LABEL_BENIGN]
                )
            )

        return inferences

    @staticmethod
    def _infer_super_sub_paths(pfx_event):

        inferences = []

        if pfx_event.has_tag("no-common-monitors"):
            inferences.append(
                Inference(
                    inference_id="defcon-no-common-monitors",
                    explanation="not observed the paths for sub/super prefixes from the same monitors",
                    suspicion_level=20,
                    confidence=90,
                    labels=[LABEL_BENIGN]
                )
            )

        if pfx_event.has_tag("superpaths-include-subpaths"):
            inferences.append(
                Inference(
                    inference_id="superpaths-include-subpaths",
                    explanation="paths to superprefix include the paths to subprefix, i.e. no new paths observed",
                    suspicion_level=20,
                    confidence=90,
                    labels=[LABEL_BENIGN]
                )
            )

        if pfx_event.has_tag("sub-path-shorter"):
            inferences.append(
                Inference(
                    inference_id="sub-path-shorter",
                    explanation="the (common part of) paths toward the sub-prefix are shorter than (the common part "
                                "of) paths toward the super-prefix",
                    suspicion_level=20,
                    confidence=80,
                    labels=[LABEL_BENIGN]
                )
            )

        return inferences
