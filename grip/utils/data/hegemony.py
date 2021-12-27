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
from collections import defaultdict
from datetime import datetime, timedelta
from pprint import pprint

import requests
from future.utils import iteritems
from requests import ConnectionError
from scipy import stats, spatial


def ceil_dt(dt):
    """ Currently, hegemony is calculated every 15 minute,
    You have to query with time 0, 15, 30, 45 minutes,
    otherwise, you will receive an empty response.
    """
    if dt.minute % 15 or dt.second:
        return dt - timedelta(minutes=dt.minute % 15,
                              seconds=dt.second % 60)
    else:
        return dt


class HegemonyUtils:
    """
    IIJ AS hegemony score utility class
    """

    def __init__(self):
        self.cache = {}
        self.cached_subgraph = set()
        self.cache_ts = ""


    ########
    # Global Hegemony Score for Counting Hegemony Valleys in AS Paths
    ########

    def count_global_hegemony_valleys(self, view_ts, paths, threshold):
        """ Count number of valleys, which depth is >= th, and ASes on the valleys
        :return: num of valleys, ASes on the valley, depth
        """
        ####
        # get global hegemony paths of observed paths
        ####

        unique_ases = set()
        for path in paths:
            unique_ases.update(path)
        hegemony_scores = self.query_hegemony(timestamp=view_ts, subgraph_asn_lst=[0], asn_lst=list(unique_ases))["0"]

        # hegemony paths
        hege_paths = []
        for path in paths:
            # remove consecutive ASes
            new_path = [v for i, v in enumerate(path) if i == 0 or v != path[i - 1]]
            hege_paths.append([(asn, hegemony_scores[asn]) if asn in hegemony_scores else 0 for asn in new_path])

        if not hege_paths:
            return 0.0, []

        ####
        # look for valleys
        ####

        valleys = []
        paths_with_valleys = []
        for index, hege_path in enumerate(hege_paths):

            if len(hege_path) < 3:
                # need at least 3 hops to form a valley
                continue

            # find all peaks based on global hegemony scores
            peak_idxs = []
            for i in range(1, len(hege_path) - 1):
                prev_score = hege_path[i - 1][1]
                curr_score = hege_path[i][1]
                next_score = hege_path[i + 1][1]
                # case 1: peak at the first element
                if i == 1 and prev_score > curr_score:
                    peak_idxs.append(0)
                # case 2: peak at the current element
                if prev_score <= curr_score and curr_score >= next_score:
                    peak_idxs.append(i)
                # case 3: peak at the last element
                if i == len(hege_path) - 2 and next_score >= curr_score:
                    peak_idxs.append(i + 1)

            # count valleys
            if len(peak_idxs) <= 1:
                # one peak create no valleys
                valleys.append(0)
                continue

            valley_cnt = 0
            for i in range(1, len(peak_idxs)):
                minimum = sorted(hege_path, key=lambda x: x[1])[0][1]

                # calculate each depth between each peak and minimum
                try:
                    depth_before_bottom = (hege_path[peak_idxs[i - 1]][1] - minimum) / float(
                        hege_path[peak_idxs[i - 1]][1])
                    depth_after_bottom = (hege_path[peak_idxs[i]][1] - minimum) / float(hege_path[peak_idxs[i]][1])
                    avg_depth = (depth_before_bottom + depth_after_bottom) / 2.0
                    # we count depth, only when it is larger than threshold
                    if avg_depth >= threshold:
                        valley_cnt += 1
                except ZeroDivisionError:
                    pass
            valleys.append(valley_cnt)
            # save valley paths for record keeping
            if valley_cnt > 0:
                paths_with_valleys.append(hege_path)

        avg_valleys = sum(valleys) / float(len(hege_paths))
        return avg_valleys, paths_with_valleys

    ########
    # Global Hegemony Score for Counting Hegemony Valleys in AS Paths
    ########

    @staticmethod
    def get_hegemony_metric(param):
        (scope, counter), peersPerASN, alpha, forceTrim = param

        if scope.startswith("{"):
            # TODO handle set origins
            # could at least accept sets with only one AS!
            return None

        # # logging.debug("(AS hegemony) computing hegemony for graph %s" % asn)
        asHege = defaultdict(float)
        # peersTotalCount = {p:float(counter["total"][p]) for p in peers if counter["total"][p]>0}
        peerASNTotalCount = {pasn: float(sum([counter["total"][p] for p in peers])) for pasn, peers in
                             iteritems(peersPerASN)}

        for asn in counter["asn"].iterkeys():
            # Don't do that: (for very distributed asn we want to report at least
            # the origin AS
            # if asn==scope:
            # continue

            # Compute betweenness centrality for each peer ASN
            allScores = [
                sum([counter["asn"][asn][p] for p in peers]) / peerASNTotalCount[pasn] if peerASNTotalCount[
                                                                                              pasn] > 0 else 0
                for pasn, peers in iteritems(peersPerASN)]

            # Adaptively filter low/high betweeness centrality scores
            peerShare = 1.0 / len(allScores)
            if forceTrim and peerShare > alpha and len(allScores) > 2:
                # Force trimming: Remove the top and bottom peer
                hege = float(stats.trim_mean(allScores, peerShare))
            else:
                hege = float(stats.trim_mean(allScores, alpha))

            # Ignore ASN with hegemony = 0: This is useful for having a smaller db
            # file, so it should be done there

            asHege[asn] = hege

        return scope, asHege

    def get_local_hege_path(self, view_ts, paths):
        # get timestamp ready for sending a query
        # we will refer scores 1 hour before the event

        paths_by_origin = {}
        for path in paths:
            if path[-1] not in paths_by_origin:
                paths_by_origin[path[-1]] = []
            paths_by_origin[path[-1]].append(path)

        hegemony = self.query_hegemony(
            timestamp=view_ts, subgraph_asn_lst=paths_by_origin.keys(),
            asn_lst=[]
        )

        # hegemony paths
        hege_paths = []
        for origin_as in paths_by_origin.keys():
            if origin_as not in hegemony:
                logging.info("No local hegemony results of origin AS, %s" % origin_as)
                continue
            for path in paths_by_origin[origin_as]:
                # remove consecutive ASes
                new_path = [v for i, v in enumerate(path) if i == 0 or v != path[i - 1]]
                hege_paths.append([hegemony[origin_as][asn]
                                   if asn in hegemony[origin_as] else 0 for asn in new_path])

        return hege_paths, hegemony

    def recalculate_local_hegemony(self, paths):
        p_ip = 0  # since we don't know peers' ips, use arbitrary ip
        perOrgASN = dict()
        for path in paths:
            origin_as = path[-1]
            peer_as = path[0]
            if origin_as not in perOrgASN:
                perOrgASN[origin_as] = {
                    'peersPerASN': dict(), 'counter': {'total': dict(), 'asn': dict()}}
            # peersPerASN
            if peer_as not in perOrgASN[origin_as]['peersPerASN']:
                perOrgASN[origin_as]['peersPerASN'][peer_as] = []
            perOrgASN[origin_as]['peersPerASN'][peer_as].append(p_ip)
            # total
            if p_ip not in perOrgASN[origin_as]['counter']['total']:
                perOrgASN[origin_as]['counter']['total'][p_ip] = 0
            perOrgASN[origin_as]['counter']['total'][p_ip] += 1
            # asn
            for asn in path:
                if asn not in perOrgASN[origin_as]['counter']['asn']:
                    perOrgASN[origin_as]['counter']['asn'][asn] = dict()
                if p_ip not in perOrgASN[origin_as]['counter']['asn'][asn]:
                    perOrgASN[origin_as]['counter']['asn'][asn][p_ip] = 0
                perOrgASN[origin_as]['counter']['asn'][asn][p_ip] += 1
            p_ip += 1

        alpha = 0.1
        forceTrim = True
        re_hegemony = {}
        for origin_as, param in iteritems(perOrgASN):  # per origin AS
            # for all p_ip
            for asn in param['counter']['asn'].keys():
                for pasn, peers in iteritems(param['peersPerASN']):
                    for p in peers:
                        if p not in param['counter']['asn'][asn]:
                            param['counter']['asn'][asn][p] = 0
            try:
                scope, asHege = self.get_hegemony_metric(((origin_as, param['counter']),
                                                          param['peersPerASN'], alpha, forceTrim))
                for this_asn, score in iteritems(asHege):
                    if score != 0.0 and str(this_asn) != str(origin_as):
                        if origin_as not in re_hegemony:
                            re_hegemony[origin_as] = dict()
                        re_hegemony[origin_as][str(this_asn)] = score
            except:
                logging.debug(
                    "Failed to get asHegemonyMetric: %s, %s, %s" % (origin_as, param['counter'], param['peersPerASN']))
        return re_hegemony

    def calculate_similarity(self, view_ts, paths):
        """

        :param view_ts:
        :param paths:
        :return:
        """
        # get local hegemony 1 hour before, and recalculate current local hegemony
        hege_paths, web_hegemony = self.get_local_hege_path(view_ts, paths)
        re_hegemony = self.recalculate_local_hegemony(paths)

        # get top 3 and calculate similarity
        similarity = {}
        origin_ases = list(set(web_hegemony.keys()) & set(re_hegemony.keys()))
        for origin_as in origin_ases:
            web_asn = sorted(web_hegemony[origin_as], key=web_hegemony[origin_as].get, reverse=True)[:3]
            loc_asn = sorted(re_hegemony[origin_as], key=re_hegemony[origin_as].get, reverse=True)[:3]
            all_asn = list(set(web_asn + loc_asn))
            web = [web_hegemony[origin_as][i] if i in web_hegemony[origin_as] else 0.0 for i in all_asn]
            loc = [re_hegemony[origin_as][i] if i in re_hegemony[origin_as] else 0.0 for i in all_asn]
            try:
                similarity[origin_as] = 1 - spatial.distance.cosine(web, loc)
            except TypeError as e:
                raise e
            except FloatingPointError as e:
                # it's likely that web or loc is all zero list, e.g. [0,0,0]
                pass

        # extra for further improvement
        avg_hege = []
        for path in hege_paths:
            if len(path) < 3:
                continue

            avg = sum(path[1:-1]) / float(len(path[1:-1]))
            avg_hege.append(avg)
        if len(avg_hege) != 0:
            avg_hege = sum(avg_hege) / float(len(avg_hege))
        else:
            avg_hege = None

        return similarity, avg_hege

    def query_hegemony(self, timestamp, subgraph_asn_lst, asn_lst):
        """
        send query to get hegemony value for ASes
        :param subgraph_asn_lst: [0] for global graph
        :param asn_lst: list of asns to filter by
        :param timestamp: timestamp in unix time
        :return:
        """

        def _extract_data(data, subgraph_origins, target_origins):
            res_dict = {}
            for subgraph_origin in subgraph_origins:
                res_dict[subgraph_origin] = {}
                for asnumer in target_origins:
                    res_dict[subgraph_origin][asnumer] = data.get(subgraph_origin, {}).get(asnumer, 0)
            return res_dict

        res = {}

        asn_lst = [str(asn) for asn in asn_lst]
        subgraph_asn_lst = [str(asn) for asn in subgraph_asn_lst]

        t = datetime.utcfromtimestamp(int(timestamp))
        query_time_str = datetime.strftime(
            ceil_dt(t) - timedelta(hours=1),
            '%Y-%m-%dT%H:%M')

        if self.cache_ts != query_time_str:
            # if timestamp changed, clear cache
            self.cache = {}
            self.cached_subgraph = set()

        uncached = {}
        for subgraph_asn in subgraph_asn_lst:
            if subgraph_asn in self.cache:
                # if subgraph cached already
                res[subgraph_asn] = {}

                if subgraph_asn in self.cached_subgraph:
                    # if we have cached the entire subgraph beforedf:w
                    for asn in self.cache[subgraph_asn]:
                        res[subgraph_asn][asn] = self.cache[subgraph_asn][asn]
                    # continue to the next subgraph
                elif asn_lst:
                    # we want partial hegemony for the asns
                    for asn in asn_lst:
                        if asn in self.cache[subgraph_asn]:
                            res[subgraph_asn][asn] = self.cache[subgraph_asn][asn]
                        else:
                            if subgraph_asn not in uncached:
                                uncached[subgraph_asn] = set()
                            uncached[subgraph_asn].add(asn)
                else:
                    # we want entire subgraph
                    uncached[subgraph_asn] = set()
            else:
                # subgraphasn is not cached fully or partially
                if subgraph_asn not in uncached:
                    uncached[subgraph_asn] = set()
                if asn_lst:
                    # we want partial subgraph
                    for asn in asn_lst:
                        uncached[subgraph_asn].add(asn)
                else:
                    uncached[subgraph_asn] = set()

        if not uncached:
            # if all cached
            return _extract_data(res, subgraph_asn_lst, asn_lst)

        asns = set()
        for subgraphasn in uncached:
            asns.update(uncached[subgraphasn])

        subgraph_query_str = ",".join(uncached.keys())
        asns_query_str = ",".join(list(asns))

        url = "https://ihr.iijlab.net/ihr/api/hegemony/?af=4&timebin={}&format=json&originasn={}&asn={}".format(
            query_time_str, subgraph_query_str, asns_query_str
        )
        rsp_raw = ""
        try:
            logging.debug("querying {}".format(url))
            rsp_raw = requests.get(url)
            rsp = rsp_raw.json()
            if 'count' not in rsp:
                raise ValueError("query failed at %s" % url)
            if rsp['count'] > 0:
                for result in rsp['results']:
                    originasn = str(result['originasn'])
                    if originasn not in res:
                        res[originasn] = {}
                    res[originasn][str(result['asn'])] = result['hege']
            else:
                for subgraphasn in uncached:
                    res[subgraphasn] = {}
        except ConnectionError as e:
            logging.error("cannot connect to remote at %s: %s" % (url, e))
        except ValueError as e:
            logging.error("cannot parse json object: %s ; %s" % (e, rsp_raw))

        # caching results
        for subgraph_asn in subgraph_asn_lst:
            if subgraph_asn not in self.cache:
                self.cache[subgraph_asn] = {}
            if subgraph_asn not in res:
                res[subgraph_asn] = {}
            if asn_lst:
                for asn in asn_lst:
                    res[subgraph_asn][asn] = res[subgraph_asn].get(asn, 0)
                    self.cache[subgraph_asn][asn] = res[subgraph_asn][asn]
            else:
                for asn in res[subgraph_asn]:
                    self.cache[subgraph_asn][asn] = res[subgraph_asn][asn]
                self.cached_subgraph.add(subgraph_asn)
        self.cache_ts = query_time_str

        return _extract_data(res, subgraph_asn_lst, asn_lst)


if __name__ == '__main__':
    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                        # filename=LOG_FILENAME,
                        level=logging.INFO)
    util = HegemonyUtils()
    pprint(util.query_hegemony(1546442100, [9121], []))
    pprint(util.query_hegemony(1546442100, [9121, 205192], [8426, 32703, 204028]))
    pprint(util.query_hegemony(1546442100, [9121, 205192], [8426, 32703, 204028]))
    pprint(util.query_hegemony(1546442100, [9121, 205192], [8426, 32703]))
    pprint(util.query_hegemony(1546442100, [9121, 15169], [8426, 32703]))
    pprint(util.query_hegemony(1546442100, [15169], []))
    pprint(util.query_hegemony(1546442100, [9121, 15169], [8426, 32703, 1022]))
    pprint(util.query_hegemony(1546442100, [9121, 15169], [8426, 32703]))
