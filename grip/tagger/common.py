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

# Redis utility functions
import time

REDIS_AVAIL_SECONDS = 86400


def get_recent_prefix_origins(prefix, view_ts, dataset):
    """
    Lookup dataset to get the most recent origins of a given prefix.

    :param prefix: prefix in question
    :param view_ts: the maximum timestamp to lookup
    :param dataset: redis dataset or local-in-memory dataset that provides `lookup`
                    and `get_most_recent_timestamp` functions
    :return: - origins that previously announce the prefix
             - the timestamp of announcement
             - the most recent time in dataset
    """

    prefix_info, asn_info = dataset.lookup(prefix, max_ts=view_ts - 1, exact_match=True)
    redis_ts = dataset.get_most_recent_timestamp(view_ts)

    if len(asn_info) == 0:
        # information about this prefix from redis at all
        # return all origins as newcomer, empty set of old_view_origins
        return set(), None, None

    (asn, data_ts) = asn_info[0]

    old_origins_set = set()
    if asn != "":
        # the asns could be empty string if the lookup failed to find any announcements
        for asn in asn.split():
            if "{" in asn:
                # TODO: properly process ASSet
                asn = asn.replace("{", "").replace("}", "")
                old_origins_set.update(asn.split(","))
            else:
                old_origins_set.add(asn)

    return old_origins_set, data_ts, redis_ts


def get_previous_origins(
        view_ts,
        prefix,
        datasets,
        in_memory
):
    """
    get newcomers for a prefix event

    :param view_ts:
    :param prefix:
    :param datasets:
    :param in_memory:
    :return: old_origins_set, outdated
    """
    assert (isinstance(view_ts, int))

    if int(time.time()) - view_ts > REDIS_AVAIL_SECONDS:
        # if the event we are checking is older than REDIS_AVAIL_SECONDS
        # we force it to use data files instead of REDIS data
        in_memory = True

    OUTDATED = False
    if in_memory:
        # if store data in memory, first load data file first
        datasets["pfx2asn_newcomer_local"].check_and_load_data_from_timestamp(view_ts)
        dataset = datasets["pfx2asn_newcomer_local"]
    else:
        dataset = datasets["pfx2asn_newcomer"]

    (old_origins_set, data_ts, redis_recent_ts) = get_recent_prefix_origins(prefix, view_ts, dataset)

    if data_ts is None:
        # information about this prefix from redis at all
        # return all origins as newcomer, empty set of old_view_origins
        return set(), OUTDATED

    assert (redis_recent_ts is not None)

    if data_ts < view_ts - 300 and data_ts < redis_recent_ts - 300:
        # the prefix origins were updated older than 5 minutes before the event
        # also older than 5 minutes before the most recent redis update timestamp
        # --> the prefix is withdrawn, and the information is up-to-date
        # --> return all origins as newcomer, empty set of old_view_origins
        return set(), OUTDATED

    if view_ts - 300 > data_ts >= redis_recent_ts - 300:
        # the prefix origins were updated older than 5 minutes before the event
        # but not older than 5 minutes before the most recent redis update timestamp
        # --> the prefix information could be outdated
        OUTDATED = True

    return old_origins_set, OUTDATED
