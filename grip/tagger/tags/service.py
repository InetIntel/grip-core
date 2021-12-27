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

from grip.utils.data.spamhaus import AsnDrop

from flask import Flask
from flask_restful import Resource, Api

from grip.tagger.tags import tagshelper

app = Flask(__name__)
api = Api(app)


class TagsService(Resource):
    def __init__(self):
        self.tags_helper = tagshelper


class TagsEverything(TagsService):
    def __init__(self):
        super(TagsEverything, self).__init__()

    def get(self):
        return {
            "definitions": self.tags_helper.get_all_tags_json(),
            "tr_worthy": self.tags_helper.get_all_tags_worthy_json(),
        }


class TagsAll(TagsService):
    def __init__(self):
        super(TagsAll, self).__init__()

    def get(self):
        return self.tags_helper.get_all_tags_json()


class TagsAllWorthy(TagsService):
    def __init__(self):
        super(TagsAllWorthy, self).__init__()

    def get(self):
        return self.tags_helper.get_all_tags_worthy_json()


class Blacklist(TagsService):
    def __init__(self):
        super(Blacklist, self).__init__()

    def get(self):
        return {"blacklist": list(self.tags_helper.blacklist_asns)}


ASN_DROP = AsnDrop(update=False)


# noinspection PyMethodMayBeStatic
class AsnDrop(TagsService):
    def __init__(self):
        super(AsnDrop, self).__init__()

    def get(self):
        return {"asndrop": ASN_DROP.get_current_list()}


api.add_resource(TagsEverything, '/tags')
api.add_resource(TagsAll, '/tags_all')
api.add_resource(TagsAllWorthy, '/tags_worthy')
api.add_resource(Blacklist, '/blacklist')
api.add_resource(AsnDrop, '/asndrop')


def main():
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    app.run(debug=False, host="0.0.0.0")


if __name__ == '__main__':
    main()
