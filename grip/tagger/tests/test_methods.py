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
import os
import time
from unittest import TestCase

from grip.events.details_edges import EdgesDetails
from grip.events.details_moas import MoasDetails
from grip.events.pfxevent import PfxEvent
from grip.redis import Adjacencies, Pfx2AsNewcomer, Pfx2AsNewcomerLocal, Pfx2AsHistorical
from grip.tagger.methods import TaggingMethodology
from grip.tagger.tags import tagshelper
from grip.tagger.tags.friends import OrgFriends
from grip.utils.data.asrank import AsRankUtils
from grip.utils.data.hegemony import HegemonyUtils
from grip.utils.data.ixpinfo import IXPInfo
from grip.utils.data.reserved_prefixes import ReservedPrefixes
from grip.utils.data.rpki import RpkiUtils
from grip.utils.data.spamhaus import AsnDrop
from grip.utils.data.trusted_asns import TrustedAsns
from grip.utils.event_utils import create_dummy_event


class TestTaggingMethodology(TestCase):

    methods = TaggingMethodology(
        datasets={
            "as_rank": AsRankUtils(),
            "ixp_info": IXPInfo(),  # 2020-01-01T00:00:00Z
            "adjacencies": Adjacencies(),
            "pfx2asn_newcomer": Pfx2AsNewcomer(),
            "pfx2asn_newcomer_local": Pfx2AsNewcomerLocal(),
            "pfx2asn_historical": Pfx2AsHistorical(),
            "asndrop": AsnDrop(ts=1577836800),
            "hegemony": HegemonyUtils(),
            "trust_asns": TrustedAsns(),
            "friend_asns": OrgFriends(),
            "reserved_pfxs": ReservedPrefixes(),
            "rpki": RpkiUtils("{}/../../utils/data/rpki-test-data".format(os.path.dirname(__file__)))
        }
    )

    def test_tag_newcomer_origins(self):
        """
        Check against number of newcomer/oldcomers
        """
        TagNoNewcomer = tagshelper.get_tag("no-newcomer")
        TagAllNewcomers = tagshelper.get_tag("all-newcomers")
        TagLessOrigins = tagshelper.get_tag("less-origins")
        TagNoOriginsPrevView = tagshelper.get_tag("no-origins-prev-view")
        TagSameOriginsPrevView = tagshelper.get_tag("same-origins-prev-view")

        self.assertEqual(self.methods.tag_newcomer_origins({1, 2}, {1, 2, 3}), [TagNoNewcomer, TagLessOrigins])
        self.assertEqual(self.methods.tag_newcomer_origins({1, 2}, set()), [TagAllNewcomers, TagNoOriginsPrevView])
        self.assertEqual(self.methods.tag_newcomer_origins({1, 2}, {3, 4}), [TagAllNewcomers])
        self.assertEqual(self.methods.tag_newcomer_origins({1, 2}, {1, 2, 3}), [TagNoNewcomer, TagLessOrigins])
        self.assertEqual(self.methods.tag_newcomer_origins({1, 2}, {1, 2}), [TagNoNewcomer, TagSameOriginsPrevView])
        self.assertEqual(self.methods.tag_newcomer_origins({1, 4}, {1, 2, 3}), [TagLessOrigins])
        self.assertEqual(self.methods.tag_newcomer_origins({1, 2}, {2}), [])

    def test_tag_asns(self):

        # private asns tags
        TagHasPrivateAsn = tagshelper.get_tag("has-private-asn")
        TagDueToPrivateAsn = tagshelper.get_tag("due-to-private-asn")
        TagHasNewcomerPrivateAsn = tagshelper.get_tag("some-newcomers-private-asn")
        TagAllNewcomerPrivateAsn = tagshelper.get_tag("all-newcomers-private-asn")

        # dps tags
        TagHasDpsAsn = tagshelper.get_tag("has-dps-asn")
        TagDueToDpsAsn = tagshelper.get_tag("due-to-dps-asn")
        TagHasNewcomerDpsAsn = tagshelper.get_tag("some-newcomers-dps-asn")
        TagAllNewcomerDpsAsn = tagshelper.get_tag("all-newcomers-dps-asn")

        # private and dps
        TagDueToPrivateAndAsTrans = tagshelper.get_tag("due-to-private-and-as-trans")

        # as-trans
        TagHasAsTrans = tagshelper.get_tag("has-as-trans")
        TagHasNewcomerAsTrans = tagshelper.get_tag("some-newcomers-as-trans")
        TagAllNewcomerAsTrans = tagshelper.get_tag("all-newcomers-as-trans")
        TagDueToAsTrans = tagshelper.get_tag("due-to-as-trans")

        # blacklist asns
        TagBlacklistAsn = tagshelper.get_tag("blacklist-asn")
        TagSpamhausAnsDrop = tagshelper.get_tag("spamhaus-asn-drop")


        tag=self.methods.tag_asns

        PRIVATE_ASN = 64512
        # private asn tests
        self.assertEqual(self.methods.tag_asns({64512, 1}, {1}),
                         {TagHasPrivateAsn, TagHasNewcomerPrivateAsn, TagDueToPrivateAsn, TagAllNewcomerPrivateAsn})

        # should have due-to, has private newcomer
        # not moas case: 1 -> 1 64512
        # not moas case: 1 2 -> 3 64512
        # not moas case: 1 -> 2 64512
        for prev,cur in [
            ({1}, {1,PRIVATE_ASN}),
            ({1, 2}, {3,PRIVATE_ASN}),
            ({1}, {2,PRIVATE_ASN}),
            # ({1, PRIVATE_ASN, PRIVATE_ASN+1}, {PRIVATE_ASN, PRIVATE_ASN+1}),
        ]:
            self.assertIn(TagDueToPrivateAsn, tag(cur, prev))

        # should NOT have due-to
        for prev, cur in [
            ({1}, {1, 2, PRIVATE_ASN})
        ]:
            self.assertEqual(self.methods.tag_asns(cur, prev),
                             {TagHasPrivateAsn, TagHasNewcomerPrivateAsn})

        self.assertEqual(self.methods.tag_asns({64512, 1, 2}, {64512, 1}),
                         {TagHasPrivateAsn})
        self.assertEqual(self.methods.tag_asns({64512, 1}, {64512, 1, 2}),
                         {TagHasPrivateAsn})

        # dps asn tests
        self.assertEqual(self.methods.tag_asns({3549, 1}, {1}),
                         {TagHasDpsAsn, TagHasNewcomerDpsAsn, TagDueToDpsAsn, TagAllNewcomerDpsAsn})
        self.assertEqual(self.methods.tag_asns({3549, 1, 2}, {1}),
                         {TagHasDpsAsn, TagHasNewcomerDpsAsn})
        self.assertEqual(self.methods.tag_asns({3549, 1, 2}, {3549, 1}),
                         {TagHasDpsAsn})
        self.assertEqual(self.methods.tag_asns({3549, 1}, {3549, 1, 2}),
                         {TagHasDpsAsn})

        self.assertEqual(self.methods.tag_asns({3549, 64512, 1, 2}, {1}),
                         {TagHasPrivateAsn, TagHasNewcomerPrivateAsn, TagHasDpsAsn, TagHasNewcomerDpsAsn})

        # as-trans tests
        self.assertEqual(self.methods.tag_asns({23456, 1}, {1}),
                         {TagHasAsTrans, TagHasNewcomerAsTrans, TagDueToAsTrans, TagAllNewcomerAsTrans})
        self.assertEqual(self.methods.tag_asns({23456, 1, 2}, {1}),
                         {TagHasAsTrans, TagHasNewcomerAsTrans})
        self.assertEqual(self.methods.tag_asns({23456, 1, 2}, {23456, 1}),
                         {TagHasAsTrans})
        self.assertEqual(self.methods.tag_asns({23456, 1}, {23456, 1, 2}),
                         {TagHasAsTrans})

        self.assertEqual(self.methods.tag_asns({11695, 1}, {1}), {TagBlacklistAsn})
        self.assertEqual(self.methods.tag_asns({3396, 1}, {1}), {TagSpamhausAnsDrop})

    def test_tag_prefixes(self):
        TagIxpPrefix = tagshelper.get_tag("ixp-prefix")
        TagReservedSpace = tagshelper.get_tag("reserved-space")
        TagShortPrefix = tagshelper.get_tag("short-prefix")
        TagLongPrefix = tagshelper.get_tag("long-prefix")
        SingleIp = tagshelper.get_tag("single-ip")

        self.assertEqual(self.methods.tag_prefixes(["80.249.208.247/32"]), [TagIxpPrefix, TagLongPrefix, SingleIp])
        self.assertEqual(self.methods.tag_prefixes(["0.0.1.2/23"]), [TagReservedSpace])
        self.assertEqual(self.methods.tag_prefixes(["0.0.1.2/9"]), [TagReservedSpace])
        self.assertEqual(self.methods.tag_prefixes(["0.0.1.2/7"]), [TagShortPrefix])

    def test_tag_historical(self):
        TagNotPreviouslyAnnounced = tagshelper.get_tag("not-previously-announced")
        TagNotPreviouslyAnnouncedByAnyNewcomer = tagshelper.get_tag("not-previously-announced-by-any-newcomer")
        TagPreviouslyAnnouncedBySomeNewcomers = tagshelper.get_tag("previously-announced-by-some-newcomers")
        TagPreviouslyAnnouncedByAllNewcomers = tagshelper.get_tag("previously-announced-by-all-newcomers")
        TagSomeNewcomerAnnouncesNoPfxs = tagshelper.get_tag("some-newcomer-announced-no-pfxs")
        TagAllNewcomerAnnouncesNoPfxs = tagshelper.get_tag("all-newcomer-announced-no-pfxs")

        self.assertEqual(self.methods.tag_historical("8.8.8.0/24", {123456, 15169}),
                         [TagPreviouslyAnnouncedBySomeNewcomers, TagSomeNewcomerAnnouncesNoPfxs])
        self.assertEqual(self.methods.tag_historical("8.8.8.0/24", {123456 }),
                         [TagNotPreviouslyAnnouncedByAnyNewcomer, TagSomeNewcomerAnnouncesNoPfxs, TagAllNewcomerAnnouncesNoPfxs])
        self.assertEqual(self.methods.tag_historical("8.8.8.0/24", {15169}),
                         [TagPreviouslyAnnouncedBySomeNewcomers, TagPreviouslyAnnouncedByAllNewcomers])
        self.assertEqual(self.methods.tag_historical("11.22.33.0/24", {15169}),
                         [TagNotPreviouslyAnnouncedByAnyNewcomer])
        self.assertEqual(self.methods.tag_historical("0.0.0.0/1", {15169}),
                         [TagNotPreviouslyAnnounced])

    def test_tag_fat_finger(self):
        TagNewcomerSmallAsn = tagshelper.get_tag("newcomer-small-asn")
        TagOriginSmallEditDistance = tagshelper.get_tag("origin-small-edit-distance")
        TagPrefixSmallEditDistance = tagshelper.get_tag("prefix-small-edit-distance")

        event = create_dummy_event("moas", ts=int(time.time()))
        self.assertEqual(self.methods.tag_fat_finger({5, 15169}, {15169}, event.pfx_events[0], "8.8.8.0/24", False),
                         [TagNewcomerSmallAsn])
        self.assertEqual(
            self.methods.tag_fat_finger({15168, 15169}, {15169}, event.pfx_events[0], "8.8.8.0/24", False),
            [TagOriginSmallEditDistance])
        self.assertEqual(self.methods.tag_fat_finger({15169, 1}, {1}, event.pfx_events[0], "8.8.9.0/24", False),
                         [TagPrefixSmallEditDistance])

    def test_tag_paths(self):
        TagPathNewcomerNeighbor = tagshelper.get_tag("all-newcomers-next-to-an-oldcomer")
        TagOldcomerOnNewcomerPaths = tagshelper.get_tag("oldcomers-always-on-newcomer-originated-paths")
        TagNewcomerOnOldcomerPaths = tagshelper.get_tag("newcomers-always-on-oldcomer-originated-paths")
        TagOldcomerPathPrepending = tagshelper.get_tag("oldcomer-path-prepending")

        self.assertEqual(self.methods.tag_paths({1, 2}, {1}, [[5, 4, 3, 1, 1, 1], [4, 3, 2]]),
                         [TagOldcomerPathPrepending])
        self.assertEqual(self.methods.tag_paths({1, 2}, {1}, [[4, 3, 1, 2]]),
                         [TagPathNewcomerNeighbor, TagOldcomerOnNewcomerPaths, TagNewcomerOnOldcomerPaths])

    def test_tag_relationships(self):
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

        self.assertEqual(self.methods.tag_relationships({"19527"}, {"16550"}),  # google siblings
                         [TagRelSomeSiblings, TagRelAllSiblings, TagRelAttackerSomeSiblings, TagRelAttackerAllSiblings,
                          TagRelAllOriginsSameCountry,
                          TagRelSomeVictimsStubAses, TagRelAllVictimsStubAses])
        self.assertEqual(self.methods.tag_relationships({"19527"}, {"16550"}),  # google siblings
                         [TagRelSomeSiblings, TagRelAllSiblings, TagRelAttackerSomeSiblings, TagRelAttackerAllSiblings,
                          TagRelAllOriginsSameCountry,
                          TagRelSomeVictimsStubAses, TagRelAllVictimsStubAses])
        self.assertEqual(self.methods.tag_relationships({"16550"}, {"19527"}),  # google siblings
                         [TagRelSomeSiblings, TagRelAllSiblings, TagRelAttackerSomeSiblings, TagRelAttackerAllSiblings,
                          TagRelAllOriginsSameCountry,
                          TagRelSomeAttackersStubAses, TagRelAllAttackersStubAses])
        self.assertEqual(self.methods.tag_relationships({"17638"}, {"4809"}),  # friends
                         [TagRelSomeSiblings, TagRelAllSiblings, TagRelAttackerSomeSiblings, TagRelAttackerAllSiblings,
                          TagRelAllOriginsSameCountry])
        self.assertEqual(self.methods.tag_relationships({"397231"}, {"12008"}),
                         [TagRelSomeSiblings, TagRelAllSiblings, TagRelAttackerSomeSiblings, TagRelAttackerAllSiblings,
                          # TagRelSingleUpstreamChain,
                          TagRelSomeAttackersAreCustomers, TagRelAllAttackersAreCustomers,
                          TagRelSomeAttackersConnected, TagRelAllAttackersConnected,
                          TagRelSomeAttackersAreDownstream, TagRelAllAttackersAreDownstream,
                          TagRelAllOriginsSameCountry
                          ])

        self.assertEqual(self.methods.tag_relationships({"12008"}, {"397231"}),
                         [TagRelSomeSiblings, TagRelAllSiblings, TagRelAttackerSomeSiblings, TagRelAttackerAllSiblings,
                          # TagRelSingleUpstreamChain,
                          TagRelSomeAttackersAreProviders, TagRelAllAttackersAreProviders,
                          TagRelSomeAttackersConnected, TagRelAllAttackersConnected,
                          TagRelSomeAttackersAreUpstream, TagRelAllAttackersAreUpstream,
                          TagRelAllOriginsSameCountry
                          ])
        self.assertEqual(self.methods.tag_relationships({"6939"}, {"15169"}),
                         [TagRelSomeAttackersArePeers, TagRelAllAttackersArePeers,
                          TagRelSomeAttackersConnected, TagRelAllAttackersConnected,
                          TagRelAllOriginsSameCountry])

    def test_tag_edges(self):
        self.methods.datasets['bi_edges_info'] = {
            "15169-19711": [1, 1],
        }
        self.methods.datasets["ixp_info"].update_ts(1577836800)

        # initialize tags
        TagNewBidirectional = tagshelper.get_tag("new-bidirectional")
        TagAdjPreviouslyObservedOpposite = tagshelper.get_tag("adj-previously-observed-opposite")
        TagAdjPreviouslyObservedExact = tagshelper.get_tag("adj-previously-observed-exact")
        TagIxpColocated = tagshelper.get_tag("ixp-colocated")
        TagEdgeSmallEditDistance = tagshelper.get_tag("edge-small-edit-distance")
        TagAllNewEdgeAtOrigin = tagshelper.get_tag("all-new-edge-at-origin")
        TagNoNewEdgeAtOrigin = tagshelper.get_tag("no-new-edge-at-origin")
        TagAllNewEdgeAtCollectors = tagshelper.get_tag("all-new-edge-at-collectors")

        self.assertEqual([TagNewBidirectional, TagAdjPreviouslyObservedExact, TagAdjPreviouslyObservedOpposite],
                         self.methods.tag_edges(
                             EdgesDetails(
                                 prefix="",
                                 as1=19711,
                                 as2=15169,
                                 aspaths_str=""
                             )))
        self.assertEqual([TagAdjPreviouslyObservedExact],
                         self.methods.tag_edges(
                             EdgesDetails(
                                 prefix="",
                                 as1=19711,
                                 as2=15133,
                                 aspaths_str=""
                             )))
        self.assertEqual([TagAdjPreviouslyObservedOpposite],
                         self.methods.tag_edges(
                             EdgesDetails(
                                 prefix="",
                                 as1=15133,
                                 as2=19711,
                                 aspaths_str=""
                             )))
        self.assertEqual([TagAdjPreviouslyObservedExact, TagAdjPreviouslyObservedOpposite, TagIxpColocated],
                         self.methods.tag_edges(
                             EdgesDetails(
                                 prefix="",
                                 as1=15169,
                                 as2=10310,
                                 aspaths_str=""
                             )))
        self.assertEqual([TagEdgeSmallEditDistance],
                         self.methods.tag_edges(
                             EdgesDetails(
                                 prefix="",
                                 as1=15169,
                                 as2=15168,
                                 aspaths_str=""
                             )))

        self.assertEqual([TagNoNewEdgeAtOrigin],
                         self.methods.tag_edges(
                             EdgesDetails(
                                 prefix="",
                                 as1=15169,
                                 as2=234,
                                 aspaths_str="1 15169 234 789"
                             )))
        self.assertEqual([TagAllNewEdgeAtOrigin],
                         self.methods.tag_edges(
                             EdgesDetails(
                                 prefix="",
                                 as1=15169,
                                 as2=234,
                                 aspaths_str="1 15169 234:1 789 15169 234"
                             )))
        self.assertEqual([TagAllNewEdgeAtOrigin],
                         self.methods.tag_edges(
                             EdgesDetails(
                                 prefix="",
                                 as1=15169,
                                 as2=234,
                                 aspaths_str="1 15169 234:1 789 234 15169"
                             )))
        self.assertEqual([TagAllNewEdgeAtCollectors],
                         self.methods.tag_edges(
                             EdgesDetails(
                                 prefix="",
                                 as1=15169,
                                 as2=234,
                                 aspaths_str="15169 234 1 2:234 15169 1 3"
                             )))

    def test_tag_common_hops(self):
        TagNoCommonMonitors = tagshelper.get_tag("no-common-monitors")
        TagSuperpathsIncludeSubpaths = tagshelper.get_tag("superpaths-include-subpaths")
        TagNoCommonHopsSubPfx = tagshelper.get_tag("no-common-hops-sub-pfx")
        TagNoCommonHopsSuperPfx = tagshelper.get_tag("no-common-hops-super-pfx")
        TagSubPathShorter = tagshelper.get_tag("sub-path-shorter")
        TagSubPathLonger = tagshelper.get_tag("sub-path-longer")
        TagSubPathEqual = tagshelper.get_tag("sub-path-equal")

        self.assertEqual([TagNoCommonMonitors],
                         self.methods.tag_common_hops([["1", "2", "3"]], [["4", "2", "3"]]))

        # check if paths to subprefix are included in paths to super prefix
        self.assertEqual([TagSuperpathsIncludeSubpaths],
                         self.methods.tag_common_hops([["1", "2", "3"]], [["1", "2", "3"]]))
        self.assertEqual([TagSuperpathsIncludeSubpaths],
                         self.methods.tag_common_hops([["1", "2", "3"], ["4", "2", "3"]], [["1", "2", "3"]]))
        self.assertEqual([TagSuperpathsIncludeSubpaths],
                         self.methods.tag_common_hops([["1", "2", "3"]], [["1", "2", "3"], ["4", "2", "3"]]))

        self.assertEqual([TagNoCommonHopsSuperPfx, TagSubPathLonger],
                         self.methods.tag_common_hops([["1", "2", "3"], ["4", "3"]],
                                                      [["1", "2", "3"], ["4", "2", "3"]]))
        self.assertEqual([TagNoCommonHopsSubPfx, TagSubPathShorter],
                         self.methods.tag_common_hops([["1", "2", "3"], ["4", "2", "3"]],
                                                      [["1", "3"], ["4", "2", "3"]]))
        self.assertEqual([TagSubPathEqual],
                         self.methods.tag_common_hops([["1", "2", "3"], ["4", "2", "3"]],
                                                      [["1", "5", "2", "3"], ["4", "2", "3"]])
                         )

    def test_tag_hegemony(self):
        TagHegemonyValleyPaths = tagshelper.get_tag("hegemony-valley-paths")
        # TagHegemonyRarePathSegments = tagshelper.get_tag("hegemony-rare-path-segments")
        self.assertEqual([TagHegemonyValleyPaths], self.methods.tag_hegemony(1577836800, [["3356", "15169", "1299"]]))

    def test_tag_notags(self):
        TagNotag = tagshelper.get_tag("notags")
        self.assertEqual([TagNotag], self.methods.tag_notags({}))
        self.assertEqual([], self.methods.tag_notags({TagNotag}))

    def test_rpki_tagging(self):
        """
        More tests of the RPKI validation is located at the grip.utils.data.rpki file tests portion.
        :return:
        """
        ts = 1617796500
        self.methods.datasets["rpki"].update_ts(ts)


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

        # new origin invalid due to wrong AS
        event_type = "moas"
        prefix = "8.8.8.0/24"
        details = MoasDetails(prefix=prefix, origins_set={15169, 12345}, old_origins_set={15169}, aspaths=[])
        pfx_event = PfxEvent(event_type=event_type, position="NEW", view_ts=ts, details=details)
        tags = sorted(self.methods.tag_rpki(pfx_event))
        self.assertListEqual(sorted([
            tags_by_group["newcomer"]["all_invalid"],
            tags_by_group["newcomer"]["some_invalid"],
            tags_by_group["oldcomer"]["all_valid"],
            tags_by_group["oldcomer"]["some_valid"],
            tags_by_group["newcomer"]["invalid_as"],
        ]), tags)

        # both new and old origins are invalid due to AS and length
        event_type = "moas"
        prefix = "8.8.8.0/25"
        details = MoasDetails(prefix=prefix, origins_set={15169, 12345}, old_origins_set={15169}, aspaths=[])
        pfx_event = PfxEvent(event_type=event_type, position="NEW", view_ts=ts, details=details)
        tags = sorted(self.methods.tag_rpki(pfx_event))
        self.assertListEqual(sorted([
            tags_by_group["newcomer"]["all_invalid"],
            tags_by_group["newcomer"]["some_invalid"],
            tags_by_group["newcomer"]["invalid_as"],
            tags_by_group["oldcomer"]["all_invalid"],
            tags_by_group["oldcomer"]["some_invalid"],
            tags_by_group["oldcomer"]["invalid_length"],
        ]), tags)
