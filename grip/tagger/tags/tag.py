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


class TagTrWorthy:
    def __init__(self, tags, worthy, explain, apply_to):
        self.tags = tags
        self.worthy = worthy
        self.explain = explain
        self.apply_to = apply_to

    def __str__(self):
        return "worthy:{}   tags:{}   explain:{} apply_to:{}".format(
            self.worthy,
            ";".join([str(t) for t in self.tags]),
            self.explain, self.apply_to)

    def __repr__(self):
        return self.__str__()

    def to_str_lst(self):
        return [t.name for t in self.tags]

    def as_dict(self):
        return {"tags": self.to_str_lst(), "worthy": self.worthy, "explain": self.explain, "apply_to": self.apply_to}


class Tag(object):
    def __init__(self, name, category, definition="", comments=None):
        self.name = name
        self.category = category
        self.definition = definition
        if not comments:
            self.comments = []
        else:
            self.comments = comments

    def to_definition(self):
        raise NotImplementedError

    def as_dict(self):
        raise NotImplementedError

    def to_json(self):
        raise NotImplementedError


class PlainTag(Tag):
    def __init__(self, name, category, definition="", comments=None):
        super().__init__(name, category, definition, comments)
        self.type = "plain"

    def __lt__(self, other):
        return self.name < other.name

    def to_definition(self):
        return {
            self.name: {
                "definition": self.definition,
                "comments": self.comments,
            }
        }

    def as_dict(self):
        return {
            "name": self.name,
        }

    @staticmethod
    def from_dict(tag_dict, category="", definition="", comments=None):
        return PlainTag(name=tag_dict["name"], category=category, definition=definition, comments=comments)

    def to_json(self):
        return json.dumps(self.as_dict())

    def __str__(self):
        return self.to_json()

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, PlainTag) and str(self) == str(other)


class ValueTag(Tag):
    def __init__(self, name, category="", definition="", comments=None, value=None):
        super().__init__(name, category, definition, comments)
        self.value = value
        self.type = "value"

    def to_definition(self):
        return {
            self.name: {
                "definition": self.definition,
                "comments": self.comments,
            }
        }

    def as_dict(self):
        return {
            "name": self.name,
            "value": self.value
        }

    @staticmethod
    def from_dict(tag_dict, category="", definition="", comments=None):
        return ValueTag(name=tag_dict["name"], value=tag_dict["value"], category=category, definition=definition, comments=comments)

    def to_json(self):
        return json.dumps(self.as_dict())

    def __str__(self):
        return self.to_json()

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, ValueTag) and str(self) == str(other)
