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

import json
import logging
import os
from copy import copy

import yaml

from grip.tagger.tags.tag import Tag, PlainTag, ValueTag, TagTrWorthy


class TaggingException(Exception):
    pass


class UseUndefinedTag(TaggingException):
    pass


class TrWorthinessUndefined(TaggingException):
    pass

DEFAULT_TAG_TYPE = "plain"
TAG_TYPE_TO_CLASS = {
    "plain": PlainTag,
    "value": ValueTag,
}

class TagsHelper(object):

    def __init__(self):
        # load tags from yaml files
        self._load_all_tags()
        self._load_all_tags_worthy()

        self.blacklist_asns = self._load_blacklist_asns()
        self.registered_tags = {}

    def get_all_tags_json(self):
        """
        Used by tags web service that returns all tags used in the system.
        """
        json = {}
        for key in self.all_tag_map:
            tag = self.all_tag_map[key]
            json[key] = {
                "definition": tag.definition,
                "comments": tag.comments,
            }
        return json

    def get_all_tags_worthy_json(self):
        """
        Used by tags web service that returns all tags used in the system.
        """
        return [x.as_dict() for x in self.tags_worthy_map]

    def get_tag(self, tag_name, raise_error=True):
        if tag_name not in self.all_tag_map:
            logging.error("use of undefined tag: %s" % tag_name)
            if raise_error:
                raise UseUndefinedTag("use of undefined tag: %s" % tag_name)
            return None
        # make a copy when return the tag, the tag could be modified later
        return copy(self.all_tag_map[tag_name])

    def parse_tag_dict(self, tag_dict):
        assert "name" in tag_dict
        t = self.all_tag_map[tag_dict["name"]]
        return TAG_TYPE_TO_CLASS[t.type].from_dict(tag_dict, t.category, t.definition, t.comments)

    # def tag_from_str(self, tag_name, raise_error=False):
    def parse_tag(self, tag_to_parse, raise_error=False):

        if isinstance(tag_to_parse, Tag):
            return tag_to_parse

        if isinstance(tag_to_parse, dict):
            return self.parse_tag_dict(tag_to_parse)

        if isinstance(tag_to_parse, str):
            # could be:
            # - just tag name: return barebone tag
            # - json in str: parse to dict
            try:
                tag_dict = json.loads(tag_to_parse)
                return self.parse_tag_dict(tag_dict)
            except:
                tag_name = tag_to_parse
                if tag_name not in self.all_tag_map:
                    # try remove prefix
                    stripped_tag = "-".join(tag_name.split("-")[1:])
                    if stripped_tag in self.all_tag_map:
                        tag_name = stripped_tag
                    else:
                        logging.error("use undefined tag: {}".format(stripped_tag))
                        if raise_error:
                            raise UseUndefinedTag(tag_name)
                        return None
                return copy(self.all_tag_map[tag_name])

        raise UseUndefinedTag("use of undefined tag: %s" % tag_to_parse)

    def check_tr_worthy(self, event_type, tags_set):
        """
        Check if a given set of tags is traceroute worthy, and return the worthy tags.
        :param event_type: event type
        :param tags_set: set of tags
        :return: (bool, list)
        """
        tag_names = {t.name for t in tags_set}
        yes_tags = set()
        no_tags = set()
        na_tags = set()
        for combination in self.tags_worthy_map:
            if event_type not in combination.apply_to:
                # event does no apply to this tr checking, skip
                continue
            current_tags = {t.name for t in combination.tags}
            if current_tags.issubset(tag_names):
                if combination.worthy == "yes":
                    # it has a yes tag combination
                    yes_tags.update(current_tags)
                elif combination.worthy == "no":
                    # it has a no tag combination
                    no_tags.update(current_tags)
                else:
                    # it has na tag combination
                    na_tags.update(current_tags)

        if len(no_tags) > 0:
            # if have some no tags, then it's not traceroute worthy
            return False, []
        elif len(yes_tags) > 0:
            # else if have some yes tags, then it's traceroute worthy
            return True, list(yes_tags)
        else:
            # otherwise, it has only na tags or no tags, it's traceroute worthy
            # note: it's not possible to have no tags, since there will be notags Tag which is a na tag itself
            return True, []

    def _load_all_tags(self):
        tag_map = {}
        with open(os.path.dirname(os.path.realpath(__file__)) + "/" + "tags.yaml", 'r') as stream:
            try:
                tags = yaml.safe_load(stream)
                for category in tags:
                    for tname in tags[category]:
                        definition = tags[category][tname]["definition"]
                        comments = tags[category][tname]["comments"]
                        ttype = tags[category][tname].get("type", DEFAULT_TAG_TYPE)
                        assert ttype in TAG_TYPE_TO_CLASS

                        tag_map[tname] = TAG_TYPE_TO_CLASS[ttype](name=tname,
                                             category=category,
                                             definition=definition,
                                             comments=comments,
                                             )
            except yaml.YAMLError as exc:
                print(exc)
        self.all_tag_map = tag_map

    def _load_all_tags_worthy(self):
        tags_worthy = []
        used_tags = set()
        with open(os.path.dirname(os.path.realpath(__file__)) + "/" + "tags_tr.yaml", 'r') as stream:
            try:
                records = yaml.safe_load(stream)
                for worthiness in records:
                    worthy = str(worthiness).lstrip("tr_")
                    for d in records[worthiness]:
                        if 'tags' not in d:
                            continue

                        if "explain" in d:
                            explain = d['explain']
                        else:
                            explain = ''
                        tags = [self.all_tag_map[t] for t in d['tags']]
                        used_tags.update(tags)

                        if "apply_to" in d:
                            apply_to = d["apply_to"]
                        else:
                            apply_to = ["moas", "submoas", "defcon", "edges"]

                        tags_worthy.append(
                            TagTrWorthy(
                                tags=tags,
                                worthy=worthy,
                                explain=explain,
                                apply_to=apply_to,
                            )
                        )
            except yaml.YAMLError as exc:
                print(exc)

        # raise alert if we have tags with missing values
        missing_tags = set(self.all_tag_map.values()) - used_tags
        if missing_tags:
            raise TrWorthinessUndefined(" ".join([str(t) for t in missing_tags]))

        self.tags_worthy_map = tags_worthy

    @staticmethod
    def _load_blacklist_asns():
        asns = set()
        with open(os.path.dirname(os.path.realpath(__file__)) + "/" + "suspicious_asns.txt", 'r') as fin:
            for line in fin:
                asns.add(int(line.strip()))
        return asns

