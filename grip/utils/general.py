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

import dateutil.parser


def to_dict(obj):
    """
    Convert any class object into a dictionary based on the class variables. If an class variable's name starts with
    "_" it will not be exported. The export process will also attempt to call as_dict() function if the field is a
    class object itself
    :param obj: object to turn into dict
    :return:
    """
    if getattr(obj, "as_dict", None):
        # override as_dict by obj
        return obj.as_dict()

    res = {}
    for key, value in obj.__dict__.items():
        if key.startswith("_"):
            continue

        if value is None or isinstance(value, (int, float, str, bool, list)):
            res[key] = value
        elif isinstance(value, set):
            res[key] = list(value)
        elif getattr(value, "as_dict", None):
            # overwrite by class
            res[key] = value.as_dict()
        else:
            res[key] = to_dict(value)
    return res


def to_json(obj):
    json.dumps(to_dict(obj))


def parse_ts(ts):
    """
    parse timestamp, return None or a Unix time in integer
    :param ts:
    :return:
    """
    if ts is None:
        return ts

    if isinstance(ts, str):
        if ts.isdigit():
            ts = int(ts)
        else:
            ts = dateutil.parser.parse(ts).strftime("%s")
    assert (isinstance(ts, int))
    return ts
