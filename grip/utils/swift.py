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
import sys
from datetime import datetime

from swiftclient.service import SwiftService, SwiftError

from grip import common


class SwiftUtils:
    def __init__(self):
        self.auth_options = common.SWIFT_AUTH_OPTIONS
        assert not any([option is None for option in self.auth_options.values()])
        self.swift = SwiftService(self.auth_options)

    def swift_files_generator(self, container_name):
        """
        Generate swift files in the specified container within start and end time range (inclusive).

        :param container_name:
        :return:
        """
        try:
            list_parts_gen = self.swift.list(container=container_name)
            for page in list_parts_gen:
                if page["success"]:
                    for item in page["listing"]:
                        yield item["name"]
                else:
                    raise page["error"]
        except SwiftError as e:
            os.error(e.value)
        except ValueError as e:
            os.error(e)
        except:
            e = sys.exc_info()[0]
            print(e)

    @staticmethod
    def get_consumer_date_hour(filename):
        # year=2017/month=12/day=13/hour=16/moas.1513182000.events.gz
        fields = filename.split("/")
        year = fields[0].split("=")[1]
        month = fields[1].split("=")[1]
        day = fields[2].split("=")[1]
        hour = fields[3].split("=")[1]
        return year, month, day, hour

    @staticmethod
    def get_consumer_filename_from_ts(event_type, ts):
        ts = int(ts)
        # 1601466300
        datestr = datetime.utcfromtimestamp(ts).strftime("year=%Y/month=%m/day=%d/hour=%H")
        file_prefix = event_type
        if event_type == "submoas" or event_type == "defcon":
            file_prefix = "subpfx-" + file_prefix
        filename = "swift://bgp-hijacks-%s/%s/%s.%s.events.gz" % (event_type, datestr, file_prefix, ts)

        return filename
