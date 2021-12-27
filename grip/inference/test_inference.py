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
import unittest

from grip.inference import Inference, InferenceResult, InferenceEngine
from grip.inference.inference_collector import InferenceCollector
from grip.utils.event_utils import create_dummy_event


class TestInference(unittest.TestCase):
    """
    Test basic functionalities of the Inference class, like sorting, hashing, and comparing.
    """

    inferences = [
        Inference(inference_id="90,20", confidence=90, suspicion_level=20),
        Inference(inference_id="80,20", confidence=80, suspicion_level=20),
        Inference(inference_id="90,30", confidence=90, suspicion_level=30),
        Inference(inference_id="80,30", confidence=80, suspicion_level=30),
    ]

    def test_sort(self):
        """
        Inferences should be sorted by confidence first, then suspicion_level
        """
        inferences = sorted(self.inferences)
        self.assertEqual(inferences[0].inference_id, "80,20")
        self.assertEqual(inferences[1].inference_id, "80,30")
        self.assertEqual(inferences[2].inference_id, "90,20")
        self.assertEqual(inferences[3].inference_id, "90,30")

    def test_hash(self):
        """
        Test creating set of Inferences and check instance exists in a set
        """
        inference_set = set(self.inferences)

        self.assertIn(self.inferences[0], inference_set)

        # same id, same confidence, different suspicion_level
        self.assertIn(
            Inference(inference_id="90,20", confidence=90, suspicion_level=30),
            inference_set)

        # same id, different confidence, same suspicion_level
        self.assertNotIn(
            Inference(inference_id="90,20", confidence=80, suspicion_level=20),
            inference_set)

        self.assertNotIn(Inference(inference_id="not int"), inference_set)

    def test_eq(self):
        """
        Test equality check between two inferences. Two inferences are equal if they share the same inference_id
        """
        self.assertEqual(Inference(inference_id="90,20", suspicion_level=20, confidence=90), self.inferences[0])
        self.assertNotEqual(Inference(inference_id="80,20"), self.inferences[0])


class TestInferenceCollector(unittest.TestCase):
    def setUp(self):
        self.collector = InferenceCollector()
        logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                            level=logging.INFO)
        logging.getLogger('elasticsearch').setLevel(logging.INFO)

    def test_moas_transiton(self):
        event = self.collector.es_conn.get_event_by_id(event_id="moas-1602089400-212560_61317")
        self.collector.infer_event(event, to_query_asrank=True, to_query_hegemony=True)
        self.collector.es_conn.index_event(event=event, debug=True)

    def test_moas_new_tags(self):
        event = self.collector.es_conn.get_event_by_id(event_id="moas-1589760000-12008_397213_397215_397217_397218_397219_397220_397222_397223_397225_397226_397227_397231_397233_397234_397235_397238_397239_397240_397241_397242", debug=False)
        self.collector.infer_event(event)
        self.collector.es_conn.index_event(event=event, debug=True)

    def test_submoas(self):
        event = self.collector.es_conn.get_event_by_id(event_id="submoas-1600417200-13030=134326")
        self.collector.infer_event(event)
        self.collector.es_conn.index_event(event=event)

    def test_edges(self):
        event = self.collector.es_conn.get_event_by_id("edges-1589996700-53222_36351")
        self.collector.infer_event(event)
        self.collector.es_conn.index_event(event=event)
    
    def test_defcon(self):
        event = self.collector.es_conn.get_event_by_id("defcon-1607104500-4739")
        self.collector.infer_event(event)
        self.collector.es_conn.index_event(event=event, debug=True)


class TestInferenceResult(unittest.TestCase):
    """
    Test InferenceResult class
    """

    def test_primary_inference(self):
        """
        When creating a InferenceResult with a list of Inference objects, it should internally sort the list and
        assign the highest ranking Inference as the `primary_inference`.
        """
        inferences = [
            Inference(inference_id="90,20", confidence=90, suspicion_level=20),
            Inference(inference_id="80,20", confidence=80, suspicion_level=20),
            Inference(inference_id="90,30", confidence=90, suspicion_level=30),
            Inference(inference_id="80,30", confidence=80, suspicion_level=30),
        ]
        res = InferenceResult(inferences=inferences)
        self.assertEqual(res.primary_inference.suspicion_level, 30)
        self.assertEqual(res.primary_inference.confidence, 90)


class TestInferenceEngine(unittest.TestCase):
    """
    Tests for inference engine logic
    TODO: change tests to run on functions instead of general inference call.
    """

    def setUp(self):
        """
        Initialize an inference engine before each test function.
        """
        self.inference_engine = InferenceEngine()

    # def test_default_inference(self):
    #     """
    #     When no inferences are generated, the inference engine should produce an default inference based on the
    #     traceroute-worthiness of the event.
    #     :return:
    #     """
    #     event = create_dummy_event("moas", tr_worthy=False)
    #     self.inference_engine.infer_on_event(event)
    #     self.assertTrue(event.has_inference("default-not-tr-worthy"))

    #     event = create_dummy_event("moas", tr_worthy=True)
    #     self.inference_engine.infer_on_event(event)
    #     self.assertTrue(event.has_inference("default-tr-worthy"))

    def test_discard_events(self):
        event = create_dummy_event("moas", ["recurring-pfx-event"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("hide-recurring-pfx-event"))

        event = create_dummy_event("moas", ["short-prefix"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("hide-short-prefix"))

        event = create_dummy_event("moas", ["submoas-covered-by-moas-subpfx"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("hide-submoas-covered-by-moas"))
        event = create_dummy_event("moas", ["submoas-covered-by-moas-superpfx"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("hide-submoas-covered-by-moas"))

        event = create_dummy_event("moas", ["no-newcomer", "less-origins"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("hide-shrinking-event"))

        event = create_dummy_event("moas", ["no-newcomer"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("likely-reboot-causes-no-newcomer"))

        event = create_dummy_event("moas", ["short-prefix", "recurring-pfx-event"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("hide-recurring-pfx-event"))
        self.assertTrue(event.has_inference("hide-short-prefix"))

    def test_bug_events(self):
        event = create_dummy_event("moas", ["no-newcomer", "outdated-info"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("redis-outdated-causes-no-newcomer"))

        event = create_dummy_event("moas", ["no-newcomer"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("likely-reboot-causes-no-newcomer"))

    def test_private_asn(self):
        event = create_dummy_event("moas", ["due-to-private-asn"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("due-to-private-asn"))

        event = create_dummy_event("moas", ["due-to-as-trans"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("due-to-as-trans"))

        event = create_dummy_event("moas", ["due-to-private-and-as-trans"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("due-to-private-and-as-trans"))

    def test_reserved_space(self):
        event = create_dummy_event("moas", ["reserved-space"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("misconfig-private-prefix"))

    def test_siblings(self):
        event = create_dummy_event("moas", ["newcomer-all-siblings"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("sibling-origins"))

        event = create_dummy_event("moas", ["all-siblings"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("sibling-origins"))

    def test_partial_siblings(self):
        event = create_dummy_event("moas", ["newcomer-some-siblings"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("some-sibling-origins"))

        event = create_dummy_event("moas", ["newcomer-some-siblings", "newcomer-all-siblings"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("sibling-origins"))

    def test_dps_asn(self):
        event = create_dummy_event("moas", ["due-to-dps-asn"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("dps-asn"))

    def test_less_specific(self):
        event = create_dummy_event("submoas", ["newcomer-less-specific"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("newcomer-less-specific"))

        event = create_dummy_event("moas", ["newcomer-less-specific"])
        self.inference_engine.infer_on_event(event)
        self.assertFalse(event.has_inference("newcomer-less-specific"))

    def test_all_newcomers(self):
        event = create_dummy_event("moas", ["all-newcomers"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("all-newcomers"))

    def test_very_specific(self):
        event = create_dummy_event("moas", ["single-ip"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("blackholed-address"))

        event = create_dummy_event("moas", ["long-prefix"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("blackholed-prefix"))

    def test_edges(self):
        event_1 = create_dummy_event("edges")
        self.inference_engine.infer_on_event(event_1)
        self.assertTrue(any(i.inference_id == "new-one-direction-edge" for i in event_1.summary.inference_result.inferences))

    def test_moas_transition(self):
        event = create_dummy_event("moas", ["moas-transition", "moas-potential-transfer"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("moas-potential-transfer"))

        event = create_dummy_event("moas", ["moas-transition", "moas-potential-convergence"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("moas-potential-convergence"))

    def test_oldcomer_on_paths(self):
        events = [
            create_dummy_event("moas", ["oldcomers-always-on-newcomer-originated-paths"]),
            create_dummy_event("moas", ["all-newcomers-next-to-an-oldcomer"]),
            ]
        for e in events:
            self.inference_engine.infer_on_event(e)
            self.assertTrue(e.has_inference("oldcomer-on-paths"))

    def test_super_sub_paths(self):
        event = create_dummy_event("defcon", ["no-common-monitors"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("defcon-no-common-monitors"))

        event = create_dummy_event("defcon", ["superpaths-include-subpaths"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("superpaths-include-subpaths"))

        event = create_dummy_event("defcon", ["sub-path-shorter"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("sub-path-shorter"))

    def test_suspicious_asns(self):
        event = create_dummy_event("moas", ["blacklist-asn"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("suspicious-blacklist-asn"))

        event = create_dummy_event("moas", ["spamhaus-asn-drop"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("suspicious-spamhaus-asn"))

    def test_misconfig(self):
        event = create_dummy_event("moas", ["all-newcomers-next-to-an-oldcomer", "newcomer-small-asn"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("misconfig-fatfinger-prepend"))

        event = create_dummy_event("moas", ["all-newcomers-next-to-an-oldcomer"])
        self.inference_engine.infer_on_event(event)
        self.assertFalse(event.has_inference("misconfig-fatfinger-prepend"))

        event = create_dummy_event("moas", ["origin-small-edit-distance", "newcomer-some-siblings"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("sibling-close-asn"))
        self.assertFalse(event.has_inference("misconfig-fatfigner-asn"))

        event = create_dummy_event("moas", ["origin-small-edit-distance"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("misconfig-fatfinger-asn"))

        event = create_dummy_event("moas", ["prefix-small-edit-distance", "newcomer-some-siblings"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("sibling-close-prefix"))
        self.assertFalse(event.has_inference("misconfig-fatfinger-prefix"))

        event = create_dummy_event("moas", ["prefix-small-edit-distance"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("misconfig-fatfinger-prefix"))

    def test_potential_pollution(self):
        event = create_dummy_event("submoas", tags=["newcomer-less-specific"], super_pfx="8.8.8.0/16")
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("newcomer-less-specific-potential-pollution"))

        event = create_dummy_event("submoas", tags=["newcomer-less-specific"], super_pfx="8.8.8.0/17")
        self.inference_engine.infer_on_event(event)
        self.assertFalse(event.has_inference("newcomer-less-specific-potential-pollution"))

    def test_relationship(self):
        event = create_dummy_event("moas", ["some-newcomers-are-providers"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("some-newcomers-are-providers"))

        event = create_dummy_event("moas", ["all-newcomers-are-peers"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("all-newcomers-are-peers"))

        event = create_dummy_event("moas", ["all-newcomers-are-providers"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("all-newcomers-are-providers"))

        event = create_dummy_event("moas", ["all-newcomers-are-rel-upstream"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("all-newcomers-are-upstreams"))

        event = create_dummy_event("moas", ["single-rel-upstream-chain"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("customer-provider-single-chain"))

    def test_prefix(self):
        event = create_dummy_event("moas", ["not-previously-announced"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("new-prefix"))

        event = create_dummy_event("moas", ["long-prefix"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("long-prefix"))

    def test_ixp_colo_prefix(self):
        event = create_dummy_event("moas", ["ixp-colocated", "ixp-prefix"])
        self.inference_engine.infer_on_event(event)
        self.assertTrue(event.has_inference("ixp-prefix-and-colocated"))

        event = create_dummy_event("moas", ["ixp-prefix"])
        self.inference_engine.infer_on_event(event)
        self.assertFalse(event.has_inference("ixp-prefix-and-colocated"))

        event = create_dummy_event("moas", ["ixp-colocated"])
        self.inference_engine.infer_on_event(event)
        self.assertFalse(event.has_inference("ixp-prefix-and-colocated"))
