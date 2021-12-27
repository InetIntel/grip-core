"""
Provides a function to extract a list of host IPs from a list of
possibly overlapping prefixes.
The list of prefixes is read from a file provided as input, containing
a prefix per line. The list of host IPs is provided in the output file
using the following format:
<pfx>  <host ip>
Prefixes that are fully covered are not considered.
"""

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
import datetime
import logging
import os
import re

import wandio
import wandio.swift

import grip.active.ripe_atlas.target_ip_generator
import grip.coodinator.announce

INPUT_CONTAINER = "bgp-hijacks-announced-pfxs"
OUTPUT_CONTAINER = "bgp-hijacks-announced-pfxs-probe-ips"
OBJECT_TMPL = "year=%04d/month=%02d/day=%02d/hour=%02d/announced-pfxs-probe-ips.%d.%s.gz"
OUTPUT_TMPL = "swift://%s/%s"


def parse_filename(filename):
    # announced-pfxs.1517097600.w604800.gz
    basename = os.path.basename(filename)
    match = re.match(r"announced-pfxs.*\.(\d+)\.(w\d+)\.gz", basename)
    if not match:
        raise ValueError("Invalid input filename: %s" % filename)
    (ts, window) = (int(match.group(1)), match.group(2))
    return ts, window


def find_missing():
    # get a list of probe IP datasets
    done_ts = set()
    for probe_ips in wandio.swift.list(OUTPUT_CONTAINER):
        (ts, window) = parse_filename(probe_ips)
        done_ts.add(ts)
    missing = []
    for ann_pfx in wandio.swift.list(INPUT_CONTAINER):
        (ts, window) = parse_filename(os.path.basename(ann_pfx))
        if ts not in done_ts:
            missing.append("swift://%s/%s" % (INPUT_CONTAINER, ann_pfx))
    return missing


def generate_output_object(filename):
    (ts, window) = parse_filename(filename)
    pts = datetime.datetime.utcfromtimestamp(ts)
    return OBJECT_TMPL % (pts.year, pts.month, pts.day, pts.hour, ts, window)


def generate_output_filename(filename):
    objname = generate_output_object(filename)
    return OUTPUT_TMPL % (OUTPUT_CONTAINER, objname)


def pfxs_to_ip(input_file, output_file=None):
    """Extract a list of IP host addresses from a lis of prefixes
    Reads the prefixes from the input file and outputs
    for each prefix, a corresponding host IP address
    (unless the prefix is fully covered by subprefixes)
    """

    if output_file is None:
        # we're gonna stick it into swift
        output_file = generate_output_filename(input_file)

    logging.info("Generating probe IPs from %s and writing to %s" %
                 (input_file, output_file))

    ip_gen = grip.active.ripe_atlas.target_ip_generator.TargetIpGenerator()

    # load announced prefixes in a patricia trie
    logging.info("Loading announced prefixes")
    with wandio.open(input_file) as in_fh:
        for line in in_fh:
            pfx = line.strip()
            ip_gen.add_pfx(pfx)

    logging.info("Generating probe IPs")
    pfx_ip = ip_gen.get_probe_pfx_ip_map()

    with wandio.open(output_file, mode="w") as out_fh:
        for pfx in pfx_ip:
            out_fh.write("%s\t%s\n" % (pfx, pfx_ip[pfx]))


def listen(group, offset):
    listener = grip.coodinator.announce.Listener(
        offset=offset,
        group=group,
        sender_type="consumer",
        sender_name="announced-pfxs"
    )

    def process_ann(in_ann):
        pfxs_to_ip(in_ann.uri)
        logging.info("Announcing new probe-ips dataset")
        return [grip.coodinator.announce.SwiftAnnouncement(
            sender_type="tagger",
            sender_name="announced-pfxs-probe-ips",
            container=OUTPUT_CONTAINER,
            object=generate_output_object(in_ann.uri),
            timestamp=parse_filename(in_ann.uri)[0]
        )]

    grip.coodinator.announce.listen_and_announce(process_ann, listener)


def main():
    parser = argparse.ArgumentParser(description="""
    Script to generate probe IPs from an announced-prefixes dataset.
    """)
    parser.add_argument("-i", "--input", help="input file",
                        nargs="?")
    parser.add_argument("-o", "--output", help="output file",
                        nargs="?", default=None)
    parser.add_argument("-m", "--missing", action="store_true", default=False,
                        help="Output a list of announced-pfx datasets without probe-ip datasets")
    parser.add_argument("-l", "--listen", action="store_true", default=False,
                        help="Listen for announcements of new datasets.")

    parser.add_argument('-g', "--group", nargs="?",
                        default="grip-announced-pfxs-probe-ips",
                        help="Set Kafka consumer group")

    parser.add_argument('-t', '--test', action="store_true",
                        help="Use testing group")

    args = parser.parse_args()

    logging.basicConfig(level="INFO",
                        format="%(asctime)s|%(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    if args.test:
        args.group = "%s-TEST" % args.group

    if args.missing:
        missing = find_missing()
        for miss in missing:
            print(miss)
    elif args.listen:
        listen(
            group=args.group,
            offset="earliest" if args.test else "latest"
        )
    elif args.input:
        pfxs_to_ip(args.input, args.output)
    else:
        parser.print_help()
