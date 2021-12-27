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
import time

from grip.tagger.common import REDIS_AVAIL_SECONDS
from grip.tagger.tagger_defcon import DefconTagger
from grip.tagger.tagger_edges import EdgesTagger
from grip.tagger.tagger_moas import MoasTagger
from grip.tagger.tagger_submoas import SubMoasTagger
from grip.utils.swift import SwiftUtils

CLASSIFIERS = {
    "defcon": DefconTagger,
    "edges": EdgesTagger,
    "moas": MoasTagger,
    "submoas": SubMoasTagger,
}

DEFAULT_GROUP_TMPL = "grip-tagger-%s"

def main():
    parser = argparse.ArgumentParser(description="""
    BGP Hijacks Tagger runner
    """)
    parser.add_argument("-c", "--type", required=True,
                        choices=CLASSIFIERS.keys(),
                        help="Tagger type to run")

    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("-f", "--pfx-events-file",
                     help="Prefix events file to process")
    grp.add_argument("-l", "--listen", action="store_true",
                     help="Listen for new data to process")
    grp.add_argument("-t", "--timestamp",
                     help="Unix timestamp to search ")

    # parser.add_argument("-m", "--in-memory", action="store_true", default=False,
    #                    help="Use in-memory recent pfx-origins data instead of Redis")
    parser.add_argument("-F", "--force-finisher", action="store_true", default=False,
                        help="Force enable finisher")
    parser.add_argument("-V", "--force-process-view", action="store_true", default=False,
                        help="Force process view even if it's been processed before")
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Whether to enable debug mode")
    parser.add_argument("-n", "--no-cache", action="store_true", default=False,
                        help="Whether to disable caching consumer files before tagging")
    parser.add_argument('-g', "--group", nargs="?",
                        default=None,
                        help="Set Kafka consumer group")
    # options for offiste mode
    parser.add_argument("-o", "--offsite-mode", action="store_true", default=False,
                        help="Run tagging off from production site")
    parser.add_argument("-p", "--pfx2as-file", help="Prefix to AS mapping file", default=None)
    parser.add_argument("-O", "--output-file", help="Output tagged events in JSON format to this file", default=None)

    parser.add_argument('-v', '--verbose', action="store_true",
                        required=False, help='Verbose logging')

    opts, _ = parser.parse_known_args()

    logging.basicConfig(level="DEBUG" if opts.verbose else "INFO",
                        format="%(asctime)s|%(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    if opts.debug:
        logging.getLogger('elasticsearch').setLevel(logging.INFO)
    else:
        logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    if opts.group is None:
        opts.group = DEFAULT_GROUP_TMPL % opts.type

    default_enable_finisher = {
        "moas": True,
        "submoas": True,
        "defcon": False,
        "edges": False,
    }
    enable_finisher = default_enable_finisher[opts.type]
    if not opts.force_finisher and (opts.pfx_events_file or opts.timestamp):
        # for single file processing, we disable finisher
        enable_finisher = False

    tagger = CLASSIFIERS[opts.type](options={
        # "in_memory_data": opts.in_memory,
        "enable_finisher": enable_finisher,
        "debug": opts.debug,
        "verbose": opts.verbose,
        "force_process_view": opts.force_process_view,
        "offsite_mode": opts.offsite_mode,
        "pfx2as_file": opts.pfx2as_file,
        "output_file": opts.output_file,
    })

    to_cache = not opts.no_cache and not opts.offsite_mode
    if opts.pfx_events_file:
        tagger.process_consumer_file(consumer_filename=opts.pfx_events_file, cache_files=to_cache)
    elif opts.timestamp:
        # swift://bgp-hijacks-moas/year=2019/month=02/day=21/hour=14/moas.1550760000.events.gz
        if int(time.time()) - int(opts.timestamp) > REDIS_AVAIL_SECONDS:
            tagger.in_memory = True
        filename = SwiftUtils.get_consumer_filename_from_ts(opts.type, opts.timestamp)
        tagger.process_consumer_file(consumer_filename=filename, cache_files=to_cache)
    else:
        tagger.listen(
            group=opts.group,
            offset="earliest",
            cache_files=to_cache
        )


if __name__ == "__main__":
    main()
