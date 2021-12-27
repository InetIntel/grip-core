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
import argparse
import logging
import os

from grip.active.driver import ActiveProbingDriver
from grip.active.collector import ActiveProbingCollector


def start_driver():
    """
    Main entry function for active-probing driver.
    """
    parser = argparse.ArgumentParser(
        description="Utility to listen for new events and trigger active measurements.")

    # add arguments
    parser.add_argument('-l', "--listen", action="store_true",
                        help="Listen for events")
    parser.add_argument('-t', "--type", nargs="?", required=True,
                        help="Event type to listen for")
    parser.add_argument('-k', "--key", nargs="?",
                        help="Ripe atlas key")
    parser.add_argument('-c', '--count', nargs="?", type=int,
                        help="Exit after receiving n events")

    parser.add_argument('-v', '--verbose', action="store_true",
                        required=False, help='Verbose logging')
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Whether to enable debug mode")

    opts = parser.parse_args()

    logging.basicConfig(level="DEBUG" if opts.verbose else "INFO",
                        format="%(asctime)s|%(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    if opts.key is None:
        atlaskey = os.environ.get('ATLASKEY')
        assert (atlaskey is not None)
        opts.key = atlaskey

    driver = ActiveProbingDriver(opts.type, opts.key, debug=opts.debug)
    driver.listen()


def start_collector():
    """
    Main entry function for active-probing result collector.
    """
    parser = argparse.ArgumentParser(
        description="Utility to listen for new events and trigger active measurements.")

    parser.add_argument('-t', "--type", nargs="?", required=True,
                        help="Event type to listen for")
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Whether to enable debug mode")

    # add argument for list of brokers
    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                        # filename=LOG_FILENAME,
                        level=logging.INFO)

    opts = parser.parse_args()

    ActiveProbingCollector(event_type=opts.type, debug=opts.debug).listen()
