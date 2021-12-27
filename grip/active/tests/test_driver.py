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
import time
from unittest import TestCase

from grip.active.driver import ActiveProbingDriver

from grip.active.ripe_atlas.target_ip_generator import TargetIpGenerator
from grip.utils.event_utils import create_dummy_event


class TestActiveDriver(TestCase):

    def setUp(self):
        self.driver = ActiveProbingDriver("moas", debug=True)

    def test_process_event(self):
        event = create_dummy_event("moas", ts=int(time.time()), tr_worthy=True)
        self.driver.process_event(event)

    def test_listen(self):
        self.driver.listen()


class TestProbeIpGenerator(TestCase):
    def setUp(self):
        self.generator = TargetIpGenerator()

    def test_get_probe_pfx_ip_map(self):
        self.generator.add_pfx("11.0.0.0/22")
        self.generator.add_pfx("11.0.0.0/23")
        self.generator.add_pfx("11.0.0.0/24")
        probe_target_map = self.generator.get_probe_pfx_ip_map()
        self.assertEqual(
            {"11.0.0.0/24": "11.0.0.1", "11.0.0.0/23": "11.0.1.1", "11.0.0.0/22": "11.0.2.1"}, probe_target_map
        )
