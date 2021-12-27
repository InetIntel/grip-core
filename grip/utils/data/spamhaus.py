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
from unittest import TestCase

import requests
from elasticsearch import NotFoundError

from .elastic_queries import query_spamhaus_list
import grip.utils.data.elastic


class AsnDrop:
    URL = 'https://www.spamhaus.org/drop/asndrop.txt'
    LAST_MODIFIED_PREFIX = "; Last-Modified: "
    EXPIRES_PREFIX = "; Expires: "

    def __init__(self,  ts=None, update=False):
        self.elastic = grip.utils.data.elastic.ElasticConn()
        self._last_modified = None
        self._expires = None
        self.asn_drop_list = None
        self.asn_drop_set = None

        self.loaded_cache = {}
        if update:
            self.update_data()

        if ts:
            self.update_ts(ts)

    def update_ts(self, ts):
        """
        Update data for the given timestamp.
        :param ts: timestamp
        :return:
        """
        self._update_cache_if_necessary(ts)

    def get_current_list(self):
        rsp = requests.get(self.URL).content.decode('utf-8')
        _, _, _, asn_drop_set = self._parse_lines(rsp.split("\n"))
        return [int(asn) for asn in asn_drop_set]

    def _update_cache_if_necessary(self, ts):
        assert (isinstance(ts, int))
        if self.loaded_cache.get("ts", 0) == ts:
            # loaded already, skip
            return
        res = self.elastic.es.search(index="spamhaus-asn-drop",
                                     body=query_spamhaus_list(ts))
        asns = []
        if res['hits']['hits']:
            asns = [data["asn"] for data in res['hits']['hits'][0]["_source"]["data"]]

        self.loaded_cache = {
            "ts": ts,
            "set": set(asns)
        }

    def any_on_list(self, asn_lst):
        """
        Check if any ASN of the provided list is on ASNDROP
        :param asn_lst: list of asns
        :return: True if at least one of the ASN in asns is on the ASNDROP list
        """
        # data = self.elastic.es.count(index="spamhaus-asn-drop",
        #                              body=query_asns_on_spamhaus_list(asn_lst, ts))
        # return data['count'] >= 1
        # self._update_cache_if_necessary(ts)
        assert self.loaded_cache != {}
        return any([str(asn) in self.loaded_cache["set"] for asn in asn_lst])

    def _commit_data(self, check_data_exists=True):
        assert (self.asn_drop_list is not None)
        record_id = self._last_modified.strftime("%Y-%m-%d")

        if check_data_exists and self.elastic.record_exists(index="spamhaus-asn-drop",
                                                            record_id=record_id):
            # if data exist already, skip commiting
            return
        self.elastic.es.index(index="spamhaus-asn-drop", id=record_id, body={
            "last_modified": self.last_modified_utc(),
            "expires": self.expires_utc(),
            "data": self.asn_drop_list,
        })

    def commit_file(self, filepath):
        self._last_modified, self._expires, self.asn_drop_list, self.asn_drop_set = \
            self._parse_lines(open(filepath, "r").readlines())
        self._commit_data(check_data_exists=False)

    def _parse_lines(self, lines):
        last_modified = None
        expires = None
        asn_drop_list = []

        for line in lines:
            line = line.rstrip()
            try:
                if line == "":
                    continue
                if line.startswith(";"):
                    if line.startswith(self.LAST_MODIFIED_PREFIX):
                        line = line.replace(self.LAST_MODIFIED_PREFIX, "")
                        last_modified = datetime.datetime.strptime(line, "%a, %d %b %Y %H:%M:%S %Z")
                    if line.startswith(self.EXPIRES_PREFIX):
                        line = line.replace(self.EXPIRES_PREFIX, "")
                        expires = datetime.datetime.strptime(line, "%a, %d %b %Y %H:%M:%S %Z")
                    continue

                fields = line.split(";")
                asn = fields[0].lstrip("AS").strip()
                org_field = fields[1].split("|")
                org_country = org_field[0].strip()
                org_name = org_field[1].strip()
                asn_drop_list.append({
                    "asn": asn,
                    "country": org_country,
                    "name": org_name,
                })
            except IndexError as e:
                print("LINE: \"{}\"".format(line))
                raise e
        asn_drop_set = set([item["asn"] for item in asn_drop_list])

        assert (last_modified is not None)
        assert (expires is not None)

        return last_modified, expires, asn_drop_list, asn_drop_set

    def update_data(self, ts=None):

        if ts is None:
            ts = datetime.datetime.utcnow()
        elif isinstance(ts, int) or isinstance(ts, float):
            ts = datetime.datetime.utcfromtimestamp(float(ts))

        assert (isinstance(ts, datetime.datetime))

        NEED_UPDATE = (
                self.asn_drop_list is None or
                ts > self._expires or ts < self._last_modified
        )

        if not NEED_UPDATE:
            return

        retry = 1

        temp_ts = ts

        while retry >= 0:
            # check if data already exist on ElasticSearch
            es_id = temp_ts.strftime("%Y-%m-%d")
            logging.info("updating spamhaus asn-drop data for date {}".format(es_id))
            try:
                res = self.elastic.es.get(
                    index="spamhaus-asn-drop",
                    id=es_id)
                record = res["_source"]
                self.asn_drop_list = record["data"]
                self.asn_drop_set = set([item["asn"] for item in self.asn_drop_list])
                self._last_modified = datetime.datetime.strptime(record["last_modified"], "%Y-%m-%dT%H:%M:%S")
                self._expires = datetime.datetime.strptime(record["expires"], "%Y-%m-%dT%H:%M:%S")

                if ts < self._last_modified:
                    # if the last-modified timestamp is still later than the current timestamp
                    # we need to go back one day and retry
                    temp_ts = temp_ts - datetime.timedelta(days=1)
                    retry = retry - 1
                    continue
                return
            except NotFoundError:
                # data not found, try one day earlier
                temp_ts = temp_ts - datetime.timedelta(days=1)
                retry = retry - 1
                logging.info("elasticsearch entry for date {} does not exist".format(es_id))
        logging.info("data does not exist on elasticsearch, downloading from source now.")

        # data not exist on ElasticSearch, retrieve it from spamhaus directly, then commit it to elasticsearch
        self.asn_drop_list = []
        # get the most recent spamhaus asn drop list
        rsp = requests.get(self.URL).text
        self._last_modified, self._expires, self.asn_drop_list, self.asn_drop_set = self._parse_lines(
            rsp.split("\n"))
        self._commit_data()

    def as_list(self):
        return self.asn_drop_list

    def as_list_asns(self):
        return [i["asn"] for i in self.asn_drop_list]

    def expires_utc(self):
        return self._expires

    def expires_utc_str(self):
        return self._expires.strftime("%a, %d %b %Y %H:%M:%S GMT")

    def last_modified_utc(self):
        return self._last_modified

    def last_modified_utc_str(self):
        return self._last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT")


def update_spamhaus():
    logging.basicConfig(level="INFO",
                        format="%(asctime)s|%(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    parser = argparse.ArgumentParser(description="BGP Hijacks Spamhaus utils")
    parser.add_argument("-a", "--asn-drop-file", required=False,
                        help="commit local spamhaus asn-drop file")

    opts = parser.parse_args()

    if opts.asn_drop_file:
        asn_drop = AsnDrop(update=False)
        asn_drop.commit_file(opts.asn_drop_file)
    else:
        AsnDrop(update=True)


class TestSpamhaus(TestCase):
    def test_get_list(self):
        asndrop = AsnDrop(update=False)
        lst = asndrop.get_current_list()
        self.assertTrue(len(lst)>0)

    def test_update(self):
        update_spamhaus()
