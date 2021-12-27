#!/usr/bin/env python

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

import setuptools

setuptools.setup(
    name='grip-core',
    version='0.2.2',
    description='GRIP Core Package',
    url='https://github.com/CAIDA/grip-core',
    author='Mingwei Zhang, Alistair King, Chiara Orsini, Danilo Cicalese, Shinyoung Cho',
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        # available on pip
        'pywandio>=0.2.5',
        'python-dateutil',
        'py-radix',
        'six',
        'sqlalchemy',
        'psycopg2-binary',  # required by sqlalchemy
        'python-swiftclient',
        'confluent_kafka',
        'ripe.atlas.cousteau',
        'netaddr',
        'filelock',
        'fuzzywuzzy',
        'elasticsearch>=7.0.4',
        'nltk',
        'requests',
        'flask-restful',
        'numpy',
        'python-Levenshtein',
        'scipy',
        'future',
        'redis==2.10.6',
        'python-dotenv',
        # pinning the version to 2.10.6 because redis-py 3.0 introduced breaking change of it's zadd function
        # https://github.com/andymccurdy/redis-py/issues/1068
    ],
    entry_points={'console_scripts': [
        # Announce CLI tools
        "grip-announce = grip.coodinator.announce:main",

        # Redis CLI tools
        "grip-redis-pfx2as-historical = grip.redis.pfx2as_historical:main",
        "grip-redis-pfx2as-newcomer = grip.redis.pfx2as_newcomer:main",
        "grip-redis-adjacencies = grip.redis.adjacencies:main",
        "grip-redis-updater = grip.coodinator.updater:main",

        # Classifier CLI tools
        "grip-announced-pfxs-gen-probe-ips = grip.tagger.announced_pfxs_probe_ips:main",
        "grip-tagger = grip.tagger.cli:main",
        "grip-tagger-transition = grip.utils.transition:main",
        "grip-tagger-backfill = grip.utils.backfill:main",

        # Active Probing CLI tools
        "grip-active-driver = grip.active.cli:start_driver",
        "grip-active-collector = grip.active.cli:start_collector",
        # "grip-active-periscope-tr = grip.active.periscope.periscope_traceroute:main"

        # Inference CLI tools
        "grip-inference-collector = grip.inference.inference_collector:main",
        "grip-inference-runner = grip.inference.inference_runner:main",

        # Results pretty-printing tool
        "grip-pretty-print = grip.active.utils.print_single_file:main",

        # Tags information service
        "grip-tags-service = grip.tagger.tags.service:main",

        # Operational event message committer
        "grip-ops-event = grip.metrics.operational_event:main",

        # External data CLI tools
        "grip-update-spamhaus = grip.utils.data.spamhaus:update_spamhaus"
    ]}
)
