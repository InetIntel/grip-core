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

import zlib


def aspaths_as_str(aspaths, separator=":"):
    if aspaths is None:
        return ""
    res = []
    for aspath in aspaths:
        res.append(" ".join([str(asn_str) for asn_str in aspath]))
    return separator.join(res)


def aspath_as_str(aspath, separator=" "):
    if aspath is None:
        return None
    return separator.join([str(asn_str) for asn_str in aspath])


def clean_up_asns(asns_tmp):
    """
    clean up asn list:
    - unwrap asn sets
    - split on underscore-separated asns

    :param asns_tmp: temporary list of asns to be processed
    :return: cleaned up list of ASNs
    """
    asns = []
    for asn in asns_tmp:
        if "{" in asn:
            asn = asn.replace("{", "").replace("}", "")
            asns.extend(asn.split(","))
        elif "_" in asn:
            asns.extend(asn.split("_"))
        else:
            asns.append(asn)
    return asns


def aspaths_from_str(aspaths_str):
    """
    construct aspaths from string.

    - paths are separated by colon (":")
    - AS numbers within a path is separated by space (" ")

    # TODO: properly handle AS set

    :param aspaths_str: raw string of aspaths from the consumer
    :return: a list of lists of integers
    """
    if aspaths_str is None:
        return []
    return [path_str.split(" ") for path_str in aspaths_str.split(":")
            if path_str and "{" not in path_str and "_" not in path_str]


def find_common_hops(aspaths):
    """
    Find the common hops in a list of aspaths
    TODO: check logic here.
    """
    if len(aspaths) <= 1:
        # no common hops if there is only one (or zero) as path
        if aspaths:
            return aspaths[0]
        return []

    common_path = list(reversed(aspaths[0]))
    for path in aspaths[1:]:
        as_path = list(reversed(path))
        common_as = -1
        # compare common elements in the aspaths
        for i in range(0, len(as_path)):
            if i >= len(common_path):
                # if length exceeding the comparison target, break
                break
            if as_path[i] == common_path[i]:
                common_as = i
            else:
                break
        # no common_path found
        common_path = common_path[:common_as + 1]
        if not common_path:
            break
    return list(reversed(common_path))


def extract_paths(sub_aspaths, super_aspaths):
    common_monitors = {path[0] for path in sub_aspaths}\
        .intersection({path[0] for path in super_aspaths})
    if len(common_monitors) == 0:
        return [], [], []
    sub_paths = [path for path in sub_aspaths if path[0] in common_monitors]
    super_paths = [path for path in super_aspaths if path[0] in common_monitors]

    return sub_paths, super_paths, common_monitors


def paths_str_to_lists(aspaths_str):
    """extract aspaths from string to list of lists"""
    paths_lst = []
    for path_str in aspaths_str.split(":"):
        paths_lst.append(path_str.strip())
    return paths_lst


def origins_from_str(origins_str):
    if origins_str is None:
        return None
    return set(origins_str.split(" "))


def compress_aspaths_str(aspaths_str: str):
    """
    Compress AS path string
    :param aspaths_str:
    :return:
    """
    if aspaths_str is None:
        return None
    # return base64.b64encode(zlib.compress(aspaths_str.encode("ascii"), 9))
    return zlib.compress(aspaths_str.encode("ascii"), 9)


def decompress_aspaths_str(compressed_aspaths_str):
    """
    Decompress AS path string
    :param compressed_aspaths_str:
    :return:
    """
    if compressed_aspaths_str is None:
        return None
    if isinstance(compressed_aspaths_str, (list,)):
        # NOTE: this is an hack dealing with
        return ":".join(compressed_aspaths_str)
    # return zlib.decompress(base64.b64decode(compressed_aspaths_str), 32 + zlib.MAX_WBITS).decode("ascii")
    return zlib.decompress(compressed_aspaths_str).decode("ascii")


def detect_ip_version(prefix):
    if prefix is None:
        return None
    if "." in prefix:
        return 4
    elif ":" in prefix:
        return 6
    else:
        return None

