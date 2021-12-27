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

from grip.inference.inference_collector import InferenceCollector
from grip.tagger.tagger_defcon import DefconTagger
from grip.tagger.tagger_edges import EdgesTagger
from grip.tagger.tagger_moas import MoasTagger
from grip.tagger.tagger_submoas import SubMoasTagger
from grip.tagger.tags import tagshelper
from grip.utils.data.elastic import ElasticConn

from grip.utils.fs import fs_get_consumer_filename_from_ts

class TestTagger(unittest.TestCase):
    """
    Tagger class basic tests (initialization, tearing down, datasets, etc.).
    This test class also include integration test for the whole tagging process.
    """
    def setUp(self):
        self.options = {"force_process_view": True, "produce_kafka_message": False, "debug": True}
        self.es_conn = ElasticConn()
        logging.getLogger('elasticsearch').setLevel(logging.INFO)

    def test_initialization(self):
        self.moas_tagger = MoasTagger(options=self.options)
        self.submoas_tagger = SubMoasTagger(options=self.options)
        self.defcon_tagger = DefconTagger(options=self.options)
        self.edges_tagger = EdgesTagger(options=self.options)
        isinstance(self.moas_tagger, MoasTagger)
        isinstance(self.submoas_tagger, SubMoasTagger)
        isinstance(self.defcon_tagger, DefconTagger)
        isinstance(self.edges_tagger, EdgesTagger)

        self.assertTrue(self.moas_tagger.DEBUG)
        self.assertEqual(self.moas_tagger.name, "moas")
        self.assertEqual(self.moas_tagger.kafka_producer_topic, "observatory-tagger-moas-DEBUG")

    def test_processing_moas(self):
        self.moas_tagger = MoasTagger(options=self.options)
        self.moas_tagger.process_consumer_file("swift://bgp-hijacks-moas/year=2020/month=05/day=18/hour=00/moas.1589760000.events.gz")

    def test_processing_defcon_2(self):
        self.moas_tagger = DefconTagger(options=self.options)
        self.moas_tagger.process_consumer_file("swift://bgp-hijacks-defcon/year=2020/month=11/day=30/hour=17/subpfx-defcon.1606756500.events.gz")


    def test_processing_submoas(self):
        self.submoas_tagger = SubMoasTagger(options=self.options)
        self.submoas_tagger.process_consumer_file("swift://bgp-hijacks-submoas/year=2020/month=05/day=18/hour=00/subpfx-submoas.1589760000.events.gz", cache_files=True)

    def test_processing_defcon(self):
        self.defcon_tagger = DefconTagger(options=self.options)
        self.defcon_tagger.process_consumer_file("swift://bgp-hijacks-defcon/year=2020/month=05/day=18/hour=00/subpfx-defcon.1589760000.events.gz")

    def test_processing_edges(self):
        self.edges_tagger = EdgesTagger(options=self.options)
        filename = SwiftUtils.get_consumer_filename_from_ts("edges", 1601466300)
        self.edges_tagger.process_consumer_file(filename)

    def test_cases(self):
        self.moas_tagger = MoasTagger(options=self.options)
        event = self.es_conn.get_event_by_id("moas-1600863300-327999_328137_65003")

        # re-tag
        event.summary.clear_inference()
        ts = event.view_ts
        self.moas_tagger.update_datasets(ts)  # NOTE: only edges run special function to update dataset
        self.moas_tagger.methodology.prepare_for_view(ts)
        self.moas_tagger.tag_event(event)

        # assert
        self.assertTrue(event.summary.has_tag(tagshelper.get_tag("all-newcomers-are-providers")))

        # reinference
        self.collector = InferenceCollector()
        self.collector.infer_event(event)
        self.es_conn.index_event(event=event)

    def test_rpki_cases(self):
        self.moas_tagger = MoasTagger(options=self.options)
        event = self.es_conn.get_event_by_id("moas-1617839400-12479_12715", debug=True)

        # re-tag
        event.summary.clear_inference()
        ts = event.view_ts
        self.moas_tagger.update_datasets(ts)  # NOTE: only edges run special function to update dataset
        self.moas_tagger.methodology.prepare_for_view(ts)
        self.moas_tagger.tag_event(event)

        self.es_conn.index_event(event=event, debug=True)
