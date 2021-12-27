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
import itertools
import logging

from nltk import edit_distance

from grip.tagger.tags import tagshelper
from grip.utils.bgp import *
from grip.utils.data.rpki import RpkiValidationStatus


def asn_is_private(asn):
    """
    Check if an AS is private
    :param asn: AS number
    :return: true if asn is private
    """
    if str(asn).isdigit():
        asn_int = int(str(asn))
        if 64512 <= asn_int <= 65534 or 4200000000 <= asn_int <= 4294967294:
            return True
    return False


def asn_is_astrans(asn):
    """
    Check if an AS is AS_TRANS
    :param asn: AS number
    """
    return str(asn) == "23456"


def asn_should_keep(asn):
    """
    Check if an AS should be discarded
    :param asn: AS number
    """
    return not (asn_is_private(asn) or asn_is_astrans(asn))


class TaggingMethodology:
    def __init__(self, datasets):
        self.datasets = datasets
        self.tags_cache = {}  # cache that is used to reduce duplicate calls
        self.current_ts = None

    def prepare_for_view(self, view_ts):
        if view_ts != self.current_ts:
            logging.info("preparing TaggingMethodology for view {}".format(view_ts))
            self.tags_cache = {}
            self.current_ts = view_ts

    def tag_newcomer_origins(self, current_origins_set, previous_origins_set):
        """
        Check if there are newcomers in this moas event. The input origins sets may contain
        private or as_trans ASNs.

        Used by: MOAS, SUBMOAS
        """
        new_origins_set = current_origins_set - previous_origins_set

        if len(current_origins_set) == 0:
            # all origins are private asns, skip tagging
            return []

        # prepare tags
        tags = []
        TagNoNewcomer = tagshelper.get_tag("no-newcomer")
        TagAllNewcomers = tagshelper.get_tag("all-newcomers")
        TagLessOrigins = tagshelper.get_tag("less-origins")
        TagNoOriginsPrevView = tagshelper.get_tag("no-origins-prev-view")
        TagSameOriginsPrevView = tagshelper.get_tag("same-origins-prev-view")

        # Check if there is no newcomer
        # Three reasons:
        #   1. The consumer lost the history (e.g., rebooted, consumer could miss certain peers' updates for various reasons)
        #      therefore treating ongoing events as new events: all origins announced the prefix in
        #      the previous view, and thus there are no newcomers
        #   2. The redis database has outdated information (see if "outdated-info" tag is also added) and it's possible that
        #      the outdated view had the newcomer as an origin, we would thus erroneously see no newcomers
        #   3. The event is a shrinking MOAS where the current origins are a subset of the origins in the previous view
        if len(new_origins_set) == 0:
            tags.append(TagNoNewcomer)
        # check if all the ASes are newcomers
        elif len(current_origins_set) == len(new_origins_set):
            # len(new_origins_set) > 0
            # all newcomers, two possible scenarios:
            # 1. no previous origins
            # 2. previous origins have no intersection with current origins
            tags.append(TagAllNewcomers)

        if len(previous_origins_set) == 0:
            tags.append(TagNoOriginsPrevView)

        # check 3: check if the (sub)MOAS is shrinking in size
        if len(current_origins_set) < len(previous_origins_set):
            tags.append(TagLessOrigins)
        elif len(current_origins_set) > len(previous_origins_set):
            # more origins, normal for a MOAS event, do nothing here
            pass
        else:
            # len(current_origins_set) == len(previous_origins_set)
            #
            # same number of origins, now and before.
            # origins could all be the same or not.
            #
            # POTENTIAL ISSUE:
            # if the origins are all the same, the event should not be produced since
            # it's a duplicated event.
            if current_origins_set == previous_origins_set:
                tags.append(TagSameOriginsPrevView)
            # if origins are not the same, the some origins are swapped with different origins,
            # this scenario tells us not much, and we don't need to tag it.

        return tags

    def tag_prefixes(self, prefixes):
        """
        check if events contain prefixes of interests:
        - ixp prefixes
        - reserved space
        - short prefix <8
        - long prefix >25
        - single-ip

        :param prefixes:
        :return: tags
        """

        tags = []
        assert isinstance(prefixes, list)

        TagIxpPrefix = tagshelper.get_tag("ixp-prefix")
        TagReservedSpace = tagshelper.get_tag("reserved-space")
        TagShortPrefix = tagshelper.get_tag("short-prefix")
        TagLongPrefix = tagshelper.get_tag("long-prefix")
        SingleIp = tagshelper.get_tag("single-ip")

        for prefix in prefixes:
            if self.datasets["ixp_info"] and self.datasets["ixp_info"].get_ixp_prefix_match(prefix):
                tags.append(TagIxpPrefix)
            if self.datasets["reserved_pfxs"].is_reserved(prefix):
                tags.append(TagReservedSpace)
            if int(prefix.split("/")[1]) < 8:
                tags.append(TagShortPrefix)
            if int(prefix.split("/")[1]) > 24:
                tags.append(TagLongPrefix)
            if int(prefix.split("/")[1]) == 32:
                tags.append(SingleIp)

        return tags

    def tag_asns(self, current_origins_set, previous_origins_set):
        """
        check if the event is related to private/DPS/Trans/blacklisted ASes

        Note: After calling this function, we exclude private and trans (23456) ASNs from further consideration in the rest of the classification functions

        :return: tags
        """

        new_origins_set = current_origins_set - previous_origins_set

        # sanity check first
        if not all(str(asn).isdigit() for asn in current_origins_set):
            logging.warning("ASes containing non-digit characters: {}".format(current_origins_set))
            return []

        tags = []

        # Private ASN tags
        TagHasPrivateAsn = tagshelper.get_tag("has-private-asn")
        TagDueToPrivateAsn = tagshelper.get_tag("due-to-private-asn")
        TagHasNewcomerPrivateAsn = tagshelper.get_tag("some-newcomers-private-asn")
        TagAllNewcomerPrivateAsn = tagshelper.get_tag("all-newcomers-private-asn")

        # AS-Trans ASN tags
        TagHasAsTrans = tagshelper.get_tag("has-as-trans")
        TagDueToAsTrans = tagshelper.get_tag("due-to-as-trans")
        TagHasNewcomerAsTrans = tagshelper.get_tag("some-newcomers-as-trans")
        TagAllNewcomerAsTrans = tagshelper.get_tag("all-newcomers-as-trans")

        # Private and AS-Trans tags
        TagDueToPrivateAndAsTrans = tagshelper.get_tag("due-to-private-and-as-trans")
        TagAllNewcomersPrivateAndAsTrans = tagshelper.get_tag("all-newcomers-private-and-as-trans")

        # DPS ASN tags
        TagHasDpsAsn = tagshelper.get_tag("has-dps-asn")
        TagDueToDpsAsn = tagshelper.get_tag("due-to-dps-asn")
        TagHasNewcomerDpsAsn = tagshelper.get_tag("some-newcomers-dps-asn")
        TagAllNewcomerDpsAsn = tagshelper.get_tag("all-newcomers-dps-asn")

        # Blacklists Tags
        TagBlacklistAsn = tagshelper.get_tag("blacklist-asn")
        TagSpamhausAnsDrop = tagshelper.get_tag("spamhaus-asn-drop")

        ####
        # Private
        ####
        if len({asn for asn in current_origins_set if asn_is_private(asn)}) > 0:
            # there are private ASN's in the current origns set
            tags.append(TagHasPrivateAsn)

        private_newcomers = {asn for asn in new_origins_set if asn_is_private(asn)}
        if len(private_newcomers) > 0:
            # has newcomer private asn
            tags.append(TagHasNewcomerPrivateAsn)

            ####
            # check if event is triggered solely by the existence of private ASNs,
            # in other words, without private ASNs, the event would not exist
            ####

            # events that are not caused by solely by private ASNs
            # moas case: 1 -> 1 2 64512
            # moas case: 1 2 -> 1 3 64512

            # case 1: there is only one or zero non-private current origin:
            # not moas case: 1 -> 1 64512
            # not moas case: 1 2 -> 3 64512
            # not moas case: 1 -> 2 64512
            # not moas case: 1 64512 64513 -> 64512 64513
            # not moas case: 1 2 64512 -> 3 64512
            non_private_current_origins = {asn for asn in current_origins_set if not asn_is_private(asn)}
            if len(non_private_current_origins) <= 1:
                tags.append(TagDueToPrivateAsn)

            # case 2: all newcomer private asn
            # moas case: 1 2 -> 1 2 64512 (all newcomers are private, still moas)
            # moas case: 1 2 64513 -> 1 2 64512 64513
            if len(private_newcomers) == len(new_origins_set):
                tags.append(TagDueToPrivateAsn)
                tags.append(TagAllNewcomerPrivateAsn)

        ####
        # AS-Trans
        # TODO: discuss if in the private/DPS checks above we're forgetting that one of the ASNs could be 23456
        ####
        if len({asn for asn in current_origins_set if asn_is_astrans(asn)}) > 0:
            # there are as_trans ASN's in the current origns set
            tags.append(TagHasAsTrans)

        as_trans_newcomers = {asn for asn in new_origins_set if asn_is_astrans(asn)}
        if len(as_trans_newcomers) > 0:
            # has newcomer as_trans asn
            tags.append(TagHasNewcomerAsTrans)

            # same logic used for private asn tagging above
            non_as_trans_current_origins = {asn for asn in current_origins_set if not asn_is_astrans(asn)}
            if len(non_as_trans_current_origins) <= 1:
                tags.append(TagDueToAsTrans)
            if len(as_trans_newcomers) == len(new_origins_set):
                tags.append(TagDueToAsTrans)
                tags.append(TagAllNewcomerAsTrans)

        ####
        # DPS
        ####
        if len({asn for asn in current_origins_set if self.datasets["trust_asns"].is_asn_trusted(asn)}) > 0:
            tags.append(TagHasDpsAsn)

        dps_newcomers = {asn for asn in new_origins_set if self.datasets["trust_asns"].is_asn_trusted(asn)}
        if len(dps_newcomers) > 0:
            # has newcomer dps asn
            tags.append(TagHasNewcomerDpsAsn)
            if len(dps_newcomers) == len(new_origins_set):
                tags.append(TagAllNewcomerDpsAsn)

            # similar logic used for private asn tagging above, but we also remove private and as_trans ASNs
            # we consider an event is caused by DPS ASNs if after removing DPS ASNs and also private and as_trans ASNs,
            # the event would not be considered as an event anymore.
            non_dps_current_origins = {
                asn for asn in current_origins_set if not
                (self.datasets["trust_asns"].is_asn_trusted(asn) or asn_is_private(asn) or asn_is_astrans(asn))
            }
            # case 1: after removing dps, private, as_trans, there are one or zero ASNs in the current origins set
            if len(non_dps_current_origins) <= 1:
                tags.append(TagDueToDpsAsn)
            # case 2: all non-private and non-as-trans newcomers are DPS ASNs
            if len(dps_newcomers) == len(new_origins_set - private_newcomers - as_trans_newcomers):
                tags.append(TagDueToDpsAsn)

        ####
        # Private + AS_Trans
        ####
        if len(private_newcomers) > 0 and len(as_trans_newcomers) > 0:
            # has at least one private and one as_trans ASN newcomer

            # get all current origins that are not private or as_trans ASNs
            filtered_current_origins = {asn for asn in current_origins_set
                                        if not (
                        asn_is_astrans(asn) or
                        asn_is_private(asn))}

            # check if after filtering there is only one or zero ASN left in the current origins set
            if len(filtered_current_origins) <= 1:
                tags.append(TagDueToPrivateAndAsTrans)
            # check if all newcomers are private and as_trans ASNs
            if len(new_origins_set - filtered_current_origins) == len(new_origins_set):
                # all newcomers are either private, dps, or as_trans
                tags.append(TagAllNewcomersPrivateAndAsTrans)

        ####
        # Blacklists
        ####

        # our blacklist
        suspicious_asns = [int(x) for x in new_origins_set if int(x) in tagshelper.blacklist_asns]
        if len(suspicious_asns) > 0:
            tags.append(TagBlacklistAsn)

        # check spamhaus asn drop list

        if self.datasets["asndrop"] and self.datasets["asndrop"].any_on_list(list(new_origins_set)):
            tags.append(TagSpamhausAnsDrop)

        return set(tags)

    # USED_BY: moas, submoas
    def tag_historical(self, prefix, new_origins_set, in_memory=False):
        """
        check if the newcomers have announced the prefix in the past.
        """

        if isinstance(prefix, list):
            if len(prefix) == 0:
                return []
            prefix = prefix[0]

        if not self.datasets["pfx2asn_historical"]:
            logging.debug("pfx2asn_historical dataset is not available, skip tag_historical function")
            return []

        TagNotPreviouslyAnnounced = tagshelper.get_tag("not-previously-announced")
        TagNotPreviouslyAnnouncedByAnyNewcomer = tagshelper.get_tag("not-previously-announced-by-any-newcomer")
        TagPreviouslyAnnouncedBySomeNewcomers = tagshelper.get_tag("previously-announced-by-some-newcomers")
        TagPreviouslyAnnouncedByAllNewcomers = tagshelper.get_tag("previously-announced-by-all-newcomers")

        TagSomeNewcomerAnnouncesNoPfxs = tagshelper.get_tag("some-newcomer-announced-no-pfxs")
        TagAllNewcomerAnnouncesNoPfxs = tagshelper.get_tag("all-newcomer-announced-no-pfxs")

        tags = []

        lookedup_prefix, asns_info = self.datasets["pfx2asn_historical"].lookup(prefix)

        if lookedup_prefix is None or lookedup_prefix == '0.0.0.0/1':
            tags.append(TagNotPreviouslyAnnounced)
            return tags

        historical_asns = set()
        try:
            # https://stackoverflow.com/questions/14807689/python-list-comprehension-to-join-list-of-lists
            # Example data item in asns_info: ('1532563200', '1532736000', ['15169'])
            historical_asns_lists = [info[2] for info in asns_info]
            historical_asns = set(itertools.chain.from_iterable(historical_asns_lists))
        except IndexError:
            logging.warning("index error while processing history for {}".format(prefix))
        except ValueError:
            logging.warning("value error while processing history for {}".format(prefix))

        if not new_origins_set:
            # if no newcomers, do not proceed on tagging
            return tags

        announced_newcomers = {asn for asn in new_origins_set if str(asn) in historical_asns}
        if announced_newcomers:
            tags.append(TagPreviouslyAnnouncedBySomeNewcomers)
            if len(announced_newcomers) == len(new_origins_set):
                tags.append(TagPreviouslyAnnouncedByAllNewcomers)
        else:
            tags.append(TagNotPreviouslyAnnouncedByAnyNewcomer)

        ####
        # check if any newcomer does not announce any prefixes
        ####
        newcomer_dataset = "pfx2asn_newcomer"
        if in_memory:
            newcomer_dataset = "pfx2asn_newcomer_local"
        newcomers_without_previous_pfxs = {asn for asn in new_origins_set if
                                           len(self.datasets[newcomer_dataset].lookup_as(str(asn))) == 0}
        if newcomers_without_previous_pfxs:
            # some newcomer announces NO prefixes
            tags.append(TagSomeNewcomerAnnouncesNoPfxs)
            if newcomers_without_previous_pfxs == new_origins_set:
                tags.append(TagAllNewcomerAnnouncesNoPfxs)

        return tags

    # USED_BY: moas, submoas
    def tag_fat_finger(self, current_origins_set, previous_origins_set, pfx_event, typo_pfx, in_memory):
        """
        Tagging potential fat-finger events:
        1. newcomer is a very small ASN, i.e. prepending mistakes
        2. newcomer origins have very small edit distance with oldcomer origins
        3. newcomer prefixes have very small edit distance with oldcomer prefixes

        # TODO: check how to adapt this for edges tagger
        """

        new_origins_set = current_origins_set - previous_origins_set

        if len(new_origins_set) == 0:
            # if there are no newcomers or old view origins,
            # then no need to proceed to the following taggings
            return []

        tags = []

        TagNewcomerSmallAsn = tagshelper.get_tag("newcomer-small-asn")
        TagOriginSmallEditDistance = tagshelper.get_tag("origin-small-edit-distance")
        TagPrefixSmallEditDistance = tagshelper.get_tag("prefix-small-edit-distance")

        # check: prepending mistakes
        # FIXME: should we check if the second-last hop is the oldcomer? or we check it at the inference engine side?
        common_max_count = 25
        for new in new_origins_set:
            if isinstance(new, str) and not new.isdigit():
                # skip non-digits asns, like as-set
                continue
            if int(new) <= common_max_count:
                # if the newcomer is in the range of the common count for AS prepending
                tags.append(TagNewcomerSmallAsn)

        # check: edit distance of asn
        is_asn_typo = False
        common_min_ed = 1
        ed_asn = {x: None for x in new_origins_set}
        for new in new_origins_set:
            min_ed = {'distance': float('inf'), 'oldcomer': None}
            for old in previous_origins_set:
                ed = edit_distance(str(new), str(old), substitution_cost=1, transpositions=True)
                if ed < min_ed['distance']:
                    # get the minimum edit distance between a new and old-view-origins
                    min_ed['distance'] = ed
                    min_ed['oldcomer'] = old
            ed_asn[new] = min_ed
            if 0 != min_ed['distance'] <= common_min_ed:
                is_asn_typo = True
                tags.append(TagOriginSmallEditDistance)
        if is_asn_typo:
            pfx_event.extra['origin_typo'] = ed_asn

        # check: edit distance of pfx
        if int(typo_pfx.split("/")[1]) > 30:
            # performance hack: skipping checking prefix distance if prefix is smaller than /30
            return tags

        is_pfx_typo = False
        common_min_ed = 1
        ed_pfx = {x: None for x in new_origins_set}
        for new in new_origins_set:
            min_ed = {'distance': float('inf'), 'prefix': None}
            # check all pfxes announced by newcomers from the previous view (5 mins)
            lookup_time = pfx_event.view_ts - 300
            if in_memory:
                pfxs = self.datasets["pfx2asn_newcomer_local"].lookup_as(str(new))
            else:
                pfxs = self.datasets["pfx2asn_newcomer"].lookup_as(str(new), lookup_time)
            if not pfxs:
                continue
            pfxs = pfxs[0][0].split(',')
            for pfx in pfxs:
                if int(pfx.split("/")[1]) > 24:
                    # skipping previous announcements that are /25 to /32
                    continue
                ed = edit_distance(pfx, typo_pfx, substitution_cost=1, transpositions=True)
                if ed < min_ed['distance']:
                    # get the minimum edit distance between a new and old-view-origins
                    min_ed['distance'] = ed
                    min_ed['prefix'] = pfx
            ed_pfx[new] = min_ed
            if 0 != min_ed['distance'] <= common_min_ed:
                is_pfx_typo = True
                tags.append(TagPrefixSmallEditDistance)
        if is_pfx_typo:
            pfx_event.extra['pfx_typo'] = ed_pfx

        return tags

    # USED_BY: moas, submoas
    # checking relative locations of newcomer and oldcomers on the AS paths
    def tag_paths(self, current_origins_set, previous_origins_set, as_paths):
        """
        Check all AS paths and check if
        - all newcomers are next to an oldcomer
        - an oldcomer is always on that paths where some newcomer originated
        - an newcomer is always on that paths where some oldcomer originated
        - oldcomer does path prepending
        """

        new_origins_set = current_origins_set - previous_origins_set

        if len(new_origins_set) == 0 or len(previous_origins_set) == 0:
            # all tags in this function related to finding newcomers and oldcomers on the aspaths.
            # if there are no newcomers or oldcomers, we can skip this tagging function
            return []

        tags = []

        TagPathNewcomerNeighbor = tagshelper.get_tag("all-newcomers-next-to-an-oldcomer")
        TagOldcomerOnNewcomerPaths = tagshelper.get_tag("oldcomers-always-on-newcomer-originated-paths")
        TagNewcomerOnOldcomerPaths = tagshelper.get_tag("newcomers-always-on-oldcomer-originated-paths")
        TagOldcomerPathPrepending = tagshelper.get_tag("oldcomer-path-prepending")

        all_paths_neighbor = True
        all_oldcomers_on_newcomers_path = True
        all_newcomers_on_oldcomers_path = True
        oldcomer_prepending = False

        for aspath in as_paths:
            new_on_path = any([new in aspath for new in new_origins_set])
            old_on_path = any([old in aspath for old in previous_origins_set])

            if not new_on_path and not old_on_path:
                # sanity check, but this shouldn't happen
                logging.warning("aspath {} has no newcomer ({}) or oldcomer ({})".format(aspath, new_origins_set,
                                                                                         previous_origins_set))
                continue

            # check if oldcomer_paths has prepending
            if old_on_path and len(aspath) > 2:
                if aspath[-1] == aspath[-2] and aspath[-1] in previous_origins_set:
                    oldcomer_prepending = True

            # check if newcomer, oldcomer, or all on paths
            if old_on_path and not new_on_path:
                # oldcomer must be originating the path
                all_newcomers_on_oldcomers_path = False
                # NOTE: newcomer not on path, `all-newcomers-next-to-an-oldcomer` tag could still apply
            elif new_on_path and not old_on_path:
                # newcomer must be originating the path
                all_oldcomers_on_newcomers_path = False
                # newcomer originating and oldcomer not on path, `all-newcomers-next-to-an-oldcomer` tag does not apply
                all_paths_neighbor = False
            else:
                # both on path, check if they are neighboring on path
                new_idx = None
                old_idx = None
                for n in new_origins_set:
                    if n in aspath:
                        new_idx = aspath.index(n)
                for o in previous_origins_set:
                    if o in aspath:
                        old_idx = aspath.index(o)
                assert new_idx is not None
                assert old_idx is not None
                if abs(new_idx - old_idx) != 1:
                    all_paths_neighbor = False

        if all_paths_neighbor:
            # if all relevant paths
            tags.append(TagPathNewcomerNeighbor)

        if all_oldcomers_on_newcomers_path:
            tags.append(TagOldcomerOnNewcomerPaths)

        if all_newcomers_on_oldcomers_path:
            tags.append(TagNewcomerOnOldcomerPaths)

        if oldcomer_prepending:
            tags.append(TagOldcomerPathPrepending)

        return tags

    def tag_rpki(self, pfx_event):
        """
        Check ROAs for the prefix announcements.

        Checks:
        - check for each new origin the ROA status
        - check for each old origin the ROA status
        - for submoas and defcon, check only the sub-prefix, as super-prefix does not attract traffic

        :param pfx_event:
        :return:
        """

        origins = pfx_event.details.get_current_origins()
        new_origins = pfx_event.details.get_new_origins()
        old_origins = pfx_event.details.get_previous_origins()
        prefix = pfx_event.details.get_prefix_of_interest()

        tags = []

        tags_by_group = {
            "newcomer": {},
            "oldcomer": {},
        }

        for group in tags_by_group.keys():
            tags_by_group[group]["all_valid"] = tagshelper.get_tag("rpki-all-{}-valid-roa".format(group))
            tags_by_group[group]["some_valid"] = tagshelper.get_tag("rpki-some-{}-valid-roa".format(group))
            tags_by_group[group]["all_invalid"] = tagshelper.get_tag("rpki-all-{}-invalid-roa".format(group))
            tags_by_group[group]["some_invalid"] = tagshelper.get_tag("rpki-some-{}-invalid-roa".format(group))
            tags_by_group[group]["all_unknown"] = tagshelper.get_tag("rpki-all-{}-unknown-roa".format(group))
            tags_by_group[group]["some_unknown"] = tagshelper.get_tag("rpki-some-{}-unknown-roa".format(group))
            tags_by_group[group]["invalid_as"] = tagshelper.get_tag("rpki-{}-invalid-roa-due-to-as".format(group))
            tags_by_group[group]["invalid_length"] = tagshelper.get_tag("rpki-{}-invalid-roa-due-to-length".format(group))

        origin_validation_status = {}
        pfx_event.extra["rpki"] = {}

        for origin in origins | old_origins:
            origin_validation_status[origin] = self.datasets["rpki"].validate_prefix_origin(prefix, origin)

        pfx_event.extra["rpki"] = origin_validation_status

        for tag_group, group_origins in [("newcomer", new_origins), ("oldcomer", old_origins)]:
            if not group_origins:
                continue

            all_valid = True
            some_valid = False
            all_invalid = True
            some_invalid = False
            all_unknown = True
            some_unknown = False

            invalid_as = False
            invalid_length = False

            for origin in group_origins:
                status = origin_validation_status[origin]
                assert isinstance(status, RpkiValidationStatus)
                if status == RpkiValidationStatus.VALID:
                    # valid
                    some_valid = True
                    all_invalid = False
                    all_unknown = False
                elif status == RpkiValidationStatus.UNKNOWN:
                    # unknown
                    some_unknown = True
                    all_invalid = False
                    all_valid = False
                else:
                    # invalid
                    some_invalid = True
                    all_valid = False
                    all_unknown = False

                    if status == RpkiValidationStatus.INVALID_AS:
                        invalid_as=True
                    if status == RpkiValidationStatus.INVALID_LENGTH:
                        invalid_length=True

            if all_valid:
                tags.append(tags_by_group[tag_group]["all_valid"])
            if some_valid:
                tags.append(tags_by_group[tag_group]["some_valid"])
            if all_invalid:
                tags.append(tags_by_group[tag_group]["all_invalid"])
            if some_invalid:
                tags.append(tags_by_group[tag_group]["some_invalid"])
            if all_unknown:
                tags.append(tags_by_group[tag_group]["all_unknown"])
            if some_unknown:
                tags.append(tags_by_group[tag_group]["some_unknown"])
            if invalid_as:
                tags.append(tags_by_group[tag_group]["invalid_as"])
            if invalid_length:
                tags.append(tags_by_group[tag_group]["invalid_length"])

        return tags

    def tag_relationships(self, attacker_origins_set: set, victim_origins_set: set):
        """
        check the relationships between the potential attackers and the potential victims.

        1. check if origins are siblings or friends
        2. check if all are on the same provider-customer chain
        2. check individual pair of new-old AS relationships
        """

        if not attacker_origins_set:
            return []

        origins_hash = (hash(tuple(attacker_origins_set)), hash(tuple(victim_origins_set)))

        if "tag_relationships" not in self.tags_cache:
            self.tags_cache["tag_relationships"] = {}
        if origins_hash in self.tags_cache["tag_relationships"]:
            # we have tagged this sets of origins previously in the current view, return the cached tags
            return self.tags_cache["tag_relationships"][origins_hash]

        # check required datasets
        for ds in ["as_rank", "friend_asns"]:
            if ds not in self.datasets:
                logging.error("%s dataset not loaded, current self.datasets: %s" % (ds, self.datasets.keys()))
                return []

        tags = []

        # sibling tags
        TagRelAllSiblings = tagshelper.get_tag("all-siblings")
        TagRelSomeSiblings = tagshelper.get_tag("some-siblings")
        TagRelAttackerSomeSiblings = tagshelper.get_tag("newcomer-some-siblings")
        TagRelAttackerAllSiblings = tagshelper.get_tag("newcomer-all-siblings")
        # pc chain tag
        TagRelSingleUpstreamChain = tagshelper.get_tag("single-rel-upstream-chain")
        # rel tags
        TagRelSomeAttackersAreProviders = tagshelper.get_tag("some-newcomers-are-providers")
        TagRelAllAttackersAreProviders = tagshelper.get_tag("all-newcomers-are-providers")
        TagRelSomeAttackersAreCustomers = tagshelper.get_tag("some-newcomers-are-customers")
        TagRelAllAttackersAreCustomers = tagshelper.get_tag("all-newcomers-are-customers")
        TagRelSomeAttackersArePeers = tagshelper.get_tag("some-newcomers-are-peers")
        TagRelAllAttackersArePeers = tagshelper.get_tag("all-newcomers-are-peers")
        TagRelSomeAttackersConnected = tagshelper.get_tag("some-newcomers-are-rel-neighbor")
        TagRelAllAttackersConnected = tagshelper.get_tag("all-newcomers-are-rel-neighbor")

        TagRelSomeAttackersAreUpstream = tagshelper.get_tag("some-newcomers-are-rel-upstream")
        TagRelAllAttackersAreUpstream = tagshelper.get_tag("all-newcomers-are-rel-upstream")
        TagRelSomeAttackersAreDownstream = tagshelper.get_tag("some-newcomers-are-rel-downstream")
        TagRelAllAttackersAreDownstream = tagshelper.get_tag("all-newcomers-are-rel-downstream")

        TagRelAllOriginsSameCountry = tagshelper.get_tag("all-origins-same-country")

        # check if attackers and victims are stub ases
        TagRelSomeAttackersStubAses = tagshelper.get_tag("some-newcomers-stub-ases")
        TagRelAllAttackersStubAses = tagshelper.get_tag("all-newcomers-stub-ases")
        TagRelSomeVictimsStubAses = tagshelper.get_tag("some-victims-stub-ases")
        TagRelAllVictimsStubAses = tagshelper.get_tag("all-victims-stub-ases")

        ####
        # 1. Check siblings
        ####
        origins_list = list(attacker_origins_set.union(victim_origins_set))
        attacker_origin_list = list(attacker_origins_set)
        victim_origin_list = list(victim_origins_set)

        all_siblings = True
        some_siblings = False
        for i in range(0, len(origins_list) - 1):
            for j in range(i + 1, len(origins_list)):
                if not self.datasets["as_rank"].are_siblings(origins_list[i], origins_list[j]) and \
                        not self.datasets["friend_asns"].are_friends(origins_list[i], origins_list[j]):
                    all_siblings = False
                else:
                    some_siblings = True
        # add tags based on some_siblings and all_siblings
        if some_siblings:
            # some origins are siblings
            tags.append(TagRelSomeSiblings)
            if all_siblings:
                # all origins are siblings
                tags.append(TagRelAllSiblings)

        # newcomer siblings
        attacker_all_siblings = True
        attacker_some_siblings = False
        for i in range(0, len(attacker_origin_list)):
            is_sibling = False
            for j in range(0, len(victim_origin_list)):
                if self.datasets["as_rank"].are_siblings(attacker_origin_list[i], victim_origin_list[j]) or \
                        self.datasets["friend_asns"].are_friends(attacker_origin_list[i], victim_origin_list[j]):
                    attacker_some_siblings = True
                    is_sibling = True
            if not is_sibling:
                attacker_all_siblings = False
        if attacker_some_siblings:
            tags.append(TagRelAttackerSomeSiblings)
            if attacker_all_siblings:
                tags.append(TagRelAttackerAllSiblings)

        ####
        # 2. Check if the origins forms a chain of customer-provider, where for each link the customer has no other
        #    providers or peers. If the chain covers all origins, it is fine to announce prefixes for each other.
        ####

        is_single_chain = True
        chain_set = set()
        for i in range(0, len(origins_list) - 1):
            if i in chain_set:
                continue
            for j in range(i, len(origins_list)):
                if self.datasets["as_rank"].is_sole_provider(origins_list[i], origins_list[j]) or \
                        self.datasets["as_rank"].is_sole_provider(origins_list[j], origins_list[i]):
                    chain_set.update([i, j])
                    break
            if i not in chain_set:
                # if after looping through all other ases, still not found a sole-provider or sole-customer, then
                # the chain breaks and end loop now
                is_single_chain = False
                break
        # if all potential attackers and victims are part of the provider-customer chain, then
        if is_single_chain:
            tags.append(TagRelSingleUpstreamChain)

        ####
        # 3. Newcomer relationships
        #
        # loop through all newcomers
        #  - if has pc rel, mark this newcomer as "is-provider" and "is-direct"
        #  - if has cp rel, mark this newcomer as "is-customer" and "is-direct"
        #  - if has pp rel, mark this newcomer as "is-peer" and "is-direct"
        #  - if newcomer is in customer cone of oldcomer, mark as "is-downstream"
        #  - if older is in customer cone of newcomer, mark as "is-upstream"
        # loop through all newcomers
        #  - if all have "is-XX" tag, tag "moas-multi-newcomers-are-all-direct-XX"
        #  - else if some have "is-XX" tag, tag "moas-multi-some-newcomers-are-direct-XX"

        attacker_tags = {}
        for attacker in attacker_origins_set:
            temp_tags = set()
            for victim in victim_origins_set:
                # adding temp_tags for the attacker AS
                as_rel = self.datasets["as_rank"].get_relationship(attacker, victim)
                if as_rel is None:
                    continue
                # direct provider
                if as_rel == 'p-c':
                    temp_tags.add("is-provider")
                    temp_tags.add("is-direct")
                # direct customer
                if as_rel == 'c-p':
                    temp_tags.add("is-customer")
                    temp_tags.add("is-direct")
                # direct peer
                if as_rel == 'p-p':
                    temp_tags.add("is-peer")
                    temp_tags.add("is-direct")
                # customer cone
                if self.datasets["as_rank"].in_customer_cone(attacker, victim):
                    temp_tags.add("is-downstream")
                # customer cone
                if self.datasets["as_rank"].in_customer_cone(victim, attacker):
                    temp_tags.add("is-upstream")
            attacker_tags[attacker] = temp_tags

        def filter_items_by_tag(dictionary, tag):
            # key:value -> asn:{tags}
            return [key for (key, value) in dictionary.items() if tag in value]

        provider_attacker_cnt = len(filter_items_by_tag(attacker_tags, "is-provider"))
        customer_attacker_cnt = len(filter_items_by_tag(attacker_tags, "is-customer"))
        peer_attacker_cnt = len(filter_items_by_tag(attacker_tags, "is-peer"))
        direct_attacker_cnt = len(filter_items_by_tag(attacker_tags, "is-direct"))
        upstream_attacker_cnt = len(filter_items_by_tag(attacker_tags, "is-upstream"))
        downstream_attacker_cnt = len(filter_items_by_tag(attacker_tags, "is-downstream"))

        if provider_attacker_cnt > 0:
            tags.append(TagRelSomeAttackersAreProviders)
            if provider_attacker_cnt == len(attacker_origins_set):
                tags.append(TagRelAllAttackersAreProviders)

        if customer_attacker_cnt > 0:
            tags.append(TagRelSomeAttackersAreCustomers)
            if customer_attacker_cnt == len(attacker_origins_set):
                tags.append(TagRelAllAttackersAreCustomers)

        if peer_attacker_cnt > 0:
            tags.append(TagRelSomeAttackersArePeers)
            if peer_attacker_cnt == len(attacker_origins_set):
                tags.append(TagRelAllAttackersArePeers)

        if direct_attacker_cnt > 0:
            tags.append(TagRelSomeAttackersConnected)
            if direct_attacker_cnt == len(attacker_origins_set):
                tags.append(TagRelAllAttackersConnected)

        if upstream_attacker_cnt > 0:
            tags.append(TagRelSomeAttackersAreUpstream)
            if upstream_attacker_cnt == len(attacker_origins_set):
                tags.append(TagRelAllAttackersAreUpstream)

        if downstream_attacker_cnt > 0:
            tags.append(TagRelSomeAttackersAreDownstream)
            if downstream_attacker_cnt == len(attacker_origins_set):
                tags.append(TagRelAllAttackersAreDownstream)

        # check if all potential attacks and victims are registered in the same country
        countries = {self.datasets["as_rank"].get_registered_country(asn) for asn in origins_list}
        if len(countries) == 1:
            country = list(countries)[0]
            if country is not None and country != "":
                # all origins registered in the same country
                tags.append(TagRelAllOriginsSameCountry)

        all_attacker_stub = True
        some_attacker_stub = False
        for asn in attacker_origins_set:
            degree = self.datasets["as_rank"].get_degree(asn)
            if degree is None:
                all_attacker_stub = False
                continue
            if degree["customer"] == 0 and degree["provider"] == 0:
                some_attacker_stub = True
            if degree["customer"] > 0 or degree["provider"] > 0:
                all_attacker_stub = False
        if some_attacker_stub:
            tags.append(TagRelSomeAttackersStubAses)
            if all_attacker_stub:
                tags.append(TagRelAllAttackersStubAses)

        all_victim_stub = True
        some_victim_stub = False
        for asn in victim_origins_set:
            degree = self.datasets["as_rank"].get_degree(asn)
            if degree is None:
                all_victim_stub = False
                continue
            if degree["customer"] == 0 and degree["provider"] == 0:
                some_victim_stub = True
            if degree["customer"] > 0 or degree["provider"] > 0:
                all_victim_stub = False
        if some_victim_stub:
            tags.append(TagRelSomeVictimsStubAses)
            if all_victim_stub:
                tags.append(TagRelAllVictimsStubAses)

        # cache tags
        self.tags_cache["tag_relationships"][origins_hash] = tags

        return tags

    # USED_BY: defcon
    def tag_common_hops(self, super_aspaths, sub_aspaths):
        """
        Check for defcon events:
        - super and sub prefixes were not both observed by monitors
        - super prefix's paths include sub prefixes' paths
        - super/sub prefix's paths shares no common hops
        - sub-prefix's paths are shorter/longer/equal comparing to super-prefix's

        :param super_aspaths:
        :param sub_aspaths:
        :return:
        """

        # TODO: should expect aspaths without duplicate ASNs

        tags = []

        TagNoCommonMonitors = tagshelper.get_tag("no-common-monitors")
        TagSuperpathsIncludeSubpaths = tagshelper.get_tag("superpaths-include-subpaths")
        TagNoCommonHopsSubPfx = tagshelper.get_tag("no-common-hops-sub-pfx")
        TagNoCommonHopsSuperPfx = tagshelper.get_tag("no-common-hops-super-pfx")
        TagSubPathShorter = tagshelper.get_tag("sub-path-shorter")
        TagSubPathLonger = tagshelper.get_tag("sub-path-longer")
        TagSubPathEqual = tagshelper.get_tag("sub-path-equal")

        # extract paths to only examine the ones that have been seen by monitors who saw both sub and super prefixes
        sub_prefix_paths, super_prefix_paths, common_monitors = extract_paths(sub_aspaths, super_aspaths)

        if len(common_monitors) == 0:
            tags.append(TagNoCommonMonitors)
            return tags

        # check: check if all sub-prefix paths have also shown in super-prefix paths
        super_paths_set = {aspath_as_str(aspath, ";") for aspath in super_prefix_paths}
        sub_paths_set = {aspath_as_str(aspath, ";") for aspath in sub_prefix_paths}
        if len(sub_paths_set - super_paths_set) == 0:
            # sub-prefix paths all seen super-prefix paths
            tags.append(TagSuperpathsIncludeSubpaths)
            return tags

        # clean the aspath
        # TODO: check logic here.
        # find the common hops for all the subprefix aspaths
        sub_prefix_common_hops = find_common_hops(sub_prefix_paths)
        super_prefix_common_hops = find_common_hops(super_prefix_paths)

        # check: only origin in common
        if len(sub_prefix_common_hops) <= 1:
            tags.append(TagNoCommonHopsSubPfx)
        if len(super_prefix_common_hops) <= 1:
            tags.append(TagNoCommonHopsSuperPfx)

        # check: number of common ASes for the sub-prefix is smaller or equal
        # clean the aspath
        if len(sub_prefix_common_hops) < len(super_prefix_common_hops):
            tags.append(TagSubPathShorter)
        elif len(sub_prefix_common_hops) > len(super_prefix_common_hops):
            tags.append(TagSubPathLonger)
        else:
            tags.append(TagSubPathEqual)

        return tags

    # USED_BY: edges, moas, submoas, defcon
    def tag_hegemony(self, ts, paths):
        """
        Hegemony-score-based tagging.
        TODO: document high-level workflow here
        TODO: should give paths with potential attackers on them
        """
        tags = []

        TagHegemonyValleyPaths = tagshelper.get_tag("hegemony-valley-paths")

        # check global hegemony valleys in paths
        threshold_of_depth = 0.95
        threshold_of_valley = 0

        # counting average number of valleys per as path using global hegemony scores
        avg_valleys, valley_paths = self.datasets["hegemony"].count_global_hegemony_valleys(ts, paths,
                                                                                            threshold_of_depth)
        if avg_valleys > threshold_of_valley:
            # if average valleys is greater than the threshold, add tag
            tags.append(TagHegemonyValleyPaths)

        # check local hegemony similarities
        # TODO: consider revise the local hegemony code checking once it's cleared up.
        # Temporarily disabled code below:
        #
        # TagHegemonyRarePathSegments = tagshelper.get_tag("hegemony-rare-path-segments")
        # similarity, avg_hege = self.datasets["hegemony"].calculate_similarity(event.view_ts, paths)
        # threshold_of_similarity = 0.125
        # for org_as in similarity.keys():
        #     if similarity[org_as] < threshold_of_similarity:
        #         tags.append(TagHegemonyRarePathSegments)
        #         break
        # event.extra['local_similarity'] = similarity
        # event.extra['local_hege_avg'] = avg_hege

        return tags

    # USED_BY: all
    def tag_notags(self, all_tags):
        """
        Add notags Tag if no tags is found
        :param all_tags:
        :return:
        """

        TagNotag = tagshelper.get_tag("notags")
        tags = []
        if not all_tags:
            tags.append(TagNotag)
        return tags

    def tag_submoas(self, details):
        """
        Submoas-only tagging method.
        :param details: event details
        :return: list of tags
        """
        tags = []

        ########
        # Tag which prefixes (among subprefix and superprefix) in the event are originated (also/only) by newcomer ASes.
        # WARNING: We call them "newcomer prefixes" which can be confusing. This doesn't mean that the
        #          prefixes are necessarily newly seen!
        ########
        TagNoNewcomerPfxs = tagshelper.get_tag("no-newcomer-pfxs")
        TagAllNewcomerPfxs = tagshelper.get_tag("all-newcomer-pfxs")
        TagNewcomerMoreSpecific = tagshelper.get_tag("newcomer-more-specific")
        TagNewcomerLessSpecific = tagshelper.get_tag("newcomer-less-specific")
        # Tags for subMOAS with also MOASes
        TagSubmoasCausingMoasSubpfx = tagshelper.get_tag("submoas-causing-moas-subpfx")
        TagSubmoasCausingMoasSuperpfx = tagshelper.get_tag("submoas-causing-moas-superpfx")
        TagSubmoasCoveredByMoasSubpfx = tagshelper.get_tag("submoas-covered-by-moas-subpfx")
        TagSubmoasCoveredByMoasSuperpfx = tagshelper.get_tag("submoas-covered-by-moas-superpfx")

        # NOTE: newcomer_pfxs are prefixes that didn't appear before. They are announced by a newcomer but, in
        #       addition, they **might** be announced by an oldcomer too. E.g., t1: A_/8; t2: A_/8, B_/24, A_/24.
        #       Here B generates a submoas with A but the newcomer prefix /24 is also originated by A at time t2.
        # TODO: verify code+comment together mingwei+alb
        newcomer_pfxs = details.get_newcomer_prefixes()
        # TODO: add an assert that checks that this number is between 0 and 2

        # Check 1: There is no newcomer prefix
        # NOTE: the TagNoNewcomerPfxs should **never** happen unless a submoas shrinks.
        # TODO: The expanding case will be analyzed as a MOAS. The shrinking should have been caught by no-newcomer +
        #       less-origins (in the near future this case might be caught without proceeding and thus not reaching
        #       here)
        if not newcomer_pfxs:
            tags.append(TagNoNewcomerPfxs)

        # Check 2: Both the subprefix and the superprefix are newcomer prefixes
        #          I.e., they are both announced by at least one newcomer AS.
        # Warning: this doesn't mean that they were not announced by an oldcomer!
        # NOTE: If this happens, it's very likely that the event is benign because simultaneous, but there is a chance
        #       that instead they happened at different times in a 5 min bin. We deal with this in the inference engine.
        elif len(newcomer_pfxs) == 2:
            tags.append(TagAllNewcomerPfxs)

        # Check 3: the sub-prefix is announced by a newcomer
        if details.is_sub_pfx_newcomer():
            tags.append(TagNewcomerMoreSpecific)
            # Check existance of MOAS
            if len(details.get_sub_origins()) > 1:
                # Yes, we do have more than one origin for the subprefix
                tags.append(TagSubmoasCausingMoasSubpfx)
                # Now let's check if this MOAS includes at least 1 old_origin (if so, we drop the subMOAS, since the
                # event will be fairly evaluated as a MOAS) or is exclusively between newcomers (in such case we don't
                # want to skip this subMOAS, because the MOAS origins might be both from the attacker)
                if len(details.get_sub_old_origins()) > 0:
                    # newcomer is more specific + there is more than 1 sub origin + there is at least one old sub origin
                    #  we will assign a -1 to this type of events in the inference engine
                    tags.append(TagSubmoasCoveredByMoasSubpfx)

        # Check 4: the super-prefix is announced by a newcomer
        if details.is_super_pfx_newcomer():
            tags.append(TagNewcomerLessSpecific)
            # Check existance of MOAS
            if len(details.get_super_origins()) > 1:
                # Yes, we do have more than one origin for the superprefix
                tags.append(TagSubmoasCausingMoasSuperpfx)
                # Now let's check if this MOAS includes at least 1 old_origin (if so, we drop the subMOAS, since the
                # event will be fairly evaluated as a MOAS) or is exclusively between newcomers (in such case we don't
                # want to skip this subMOAS, because the MOAS origins might be both from the attacker)
                if len(details.get_super_old_origins()) > 0:
                    # newcomer is less specific + there is > 1 super origin + there is at least 1 old super origin
                    # we will assign a -1 to this type of events in the inference engine
                    tags.append(TagSubmoasCoveredByMoasSuperpfx)

        return tags

    def tag_defcon(self, details):
        """
        Defcon-only tagging method.
        :param details: event details
        :return: list of tags
        """
        tags = []
        return tags

    def tag_edges(self, details):
        """
        Edges-only tagging method.
        :param details: event details
        :return: list of tags
        """
        tags = []

        as1 = str(details.get_as1())
        as2 = str(details.get_as2())
        edgeid = "{}-{}".format(as1, as2)
        edgeid2 = "{}-{}".format(as2, as1)

        # initialize tags
        TagEdgeSmallEditDistance = tagshelper.get_tag("edge-small-edit-distance")
        TagNewBidirectional = tagshelper.get_tag("new-bidirectional")
        TagAdjPreviouslyObservedOpposite = tagshelper.get_tag("adj-previously-observed-opposite")
        TagAdjPreviouslyObservedExact = tagshelper.get_tag("adj-previously-observed-exact")
        TagIxpColocated = tagshelper.get_tag("ixp-colocated")
        TagAllNewEdgeAtOrigin = tagshelper.get_tag("all-new-edge-at-origin")
        TagNoNewEdgeAtOrigin = tagshelper.get_tag("no-new-edge-at-origin")
        TagAllNewEdgeAtCollectors = tagshelper.get_tag("all-new-edge-at-collectors")

        ####
        # cache-able tags
        ####

        if "tag_edges" not in self.tags_cache:
            self.tags_cache["tag_edges"] = {}

        if edgeid in self.tags_cache["tag_edges"]:
            tags.extend(self.tags_cache["tag_edges"][edgeid])
        else:
            to_cache = []
            # Check edit distance between the two ASes of the new edge
            common_min_ed = 1
            ed = edit_distance(as1, as2, substitution_cost=1, transpositions=True)
            if 0 != ed <= common_min_ed:
                tags.append(TagEdgeSmallEditDistance)
                to_cache.append(TagEdgeSmallEditDistance)

            # Check if the new edge was just seen in both directions
            bidirectional_edges = self.datasets["bi_edges_info"]
            if bidirectional_edges.get(edgeid, []) == [1, 1] or bidirectional_edges.get(edgeid2, []) == [1, 1]:
                tags.append(TagNewBidirectional)
                to_cache.append(TagNewBidirectional)

            # Check if we observed the edge in the past
            if self.datasets["adjacencies"]:
                if self.datasets["adjacencies"].is_neighbor_historical(as1, as2):
                    tags.append(TagAdjPreviouslyObservedExact)
                    to_cache.append(TagAdjPreviouslyObservedExact)
                if self.datasets["adjacencies"].is_neighbor_historical(as2, as1):
                    tags.append(TagAdjPreviouslyObservedOpposite)
                    to_cache.append(TagAdjPreviouslyObservedOpposite)

            # check 2: check if the edge is between two ASNs that are at the same IXP
            # event.as1, event.as2 @ same IXP
            if self.datasets["ixp_info"]:
                comm_ixps = self.datasets["ixp_info"].get_common_ixps(as1, as2)
                if comm_ixps and len(comm_ixps) > 0:
                    tags.append(TagIxpColocated)
                    to_cache.append(TagIxpColocated)
                self.tags_cache["tag_edges"][edgeid] = to_cache

        ####
        # non-cache-able tags
        ####

        # Check where the new edge is on the paths
        # "get_edge_positions_on_paths" counts the hops before and after the new edge.
        # if for all "after==0": the new edge is at the origin
        # if for all "before==0": the new edge is at the collector
        positions = details.get_edge_positions_on_paths()
        if positions:
            if all([after == 0 for before, after in positions]):
                # all edges are connected to the origins
                tags.append(TagAllNewEdgeAtOrigin)
            elif all([before == 0 for before, after in positions]):
                tags.append(TagAllNewEdgeAtCollectors)
            elif all([after > 0 for before, after in positions]):
                # all edges are connected to the origins
                tags.append(TagNoNewEdgeAtOrigin)

        return tags
