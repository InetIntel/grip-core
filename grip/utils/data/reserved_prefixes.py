#!/usr/bin/env python

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

#
# This file is part of ioda-tools
#
# CAIDA, UC San Diego
# bgpstream-info@caida.org
#
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see <http://www.gnu.org/licenses/>.
# !/usr/bin/env python
#
# This file is part of as-traceroute
#
# CAIDA, UC San Diego
# bgpstream-info@caida.org
#
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see <http://www.gnu.org/licenses/>.

# reference
# ipv4: https://tools.ietf.org/html/rfc6890
# ipv6: http://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml
# TODO: the prefixes are hardcoded, we should create a DB.

from radix import Radix


class ReservedPrefixes(object):

    def __init__(self):
        # Patricia trie for reserved prefixes ipv4
        self.__reserved_tree_ipv4 = Radix()
        self.__reserved_tree_ipv4.add("0.0.0.0/8")
        self.__reserved_tree_ipv4.add("1.1.1.0/24")
        self.__reserved_tree_ipv4.add("10.0.0.0/8")
        self.__reserved_tree_ipv4.add("100.64.0.0/10")
        self.__reserved_tree_ipv4.add("127.0.0.0/8")
        self.__reserved_tree_ipv4.add("169.254.0.0/16")
        self.__reserved_tree_ipv4.add("172.16.0.0/12")
        self.__reserved_tree_ipv4.add("192.0.0.0/24")
        self.__reserved_tree_ipv4.add("192.0.2.0/24")
        self.__reserved_tree_ipv4.add("192.88.99.0/24")
        self.__reserved_tree_ipv4.add("192.168.0.0/16")
        self.__reserved_tree_ipv4.add("198.18.0.0/15")
        self.__reserved_tree_ipv4.add("198.51.100.0/24")
        self.__reserved_tree_ipv4.add("203.0.113.0/24")
        self.__reserved_tree_ipv4.add("224.0.0.0/4")
        self.__reserved_tree_ipv4.add("240.0.0.0/4")
        self.__reserved_tree_ipv4.add("255.255.255.255/32")

        # Patricia trie for reserved prefixes ipv6
        self.__reserved_tree_ipv6 = Radix()
        self.__reserved_tree_ipv6.add("::/128")
        self.__reserved_tree_ipv6.add("::1/128")
        self.__reserved_tree_ipv6.add("::ffff:0:0/96")
        self.__reserved_tree_ipv6.add("64:ff9b::/96")
        self.__reserved_tree_ipv6.add("100::/64")
        self.__reserved_tree_ipv6.add("2001::/23")
        self.__reserved_tree_ipv6.add("2001::/32")
        self.__reserved_tree_ipv6.add("2001:1::1/128")
        self.__reserved_tree_ipv6.add("2001:1::2/128")
        self.__reserved_tree_ipv6.add("2001:2::/48")
        self.__reserved_tree_ipv6.add("2001:3::/32")
        self.__reserved_tree_ipv6.add("2001:4:112::/48")
        self.__reserved_tree_ipv6.add("2001:5::/32")
        self.__reserved_tree_ipv6.add("2001:10::/28")
        self.__reserved_tree_ipv6.add("2001:20::/28")
        self.__reserved_tree_ipv6.add("2001:db8::/32")
        self.__reserved_tree_ipv6.add("2002::/16")
        self.__reserved_tree_ipv6.add("2620:4f:8000::/48")
        self.__reserved_tree_ipv6.add("fc00::/7")
        self.__reserved_tree_ipv6.add("fe80::/10")

    def is_reserved(self, prefix):
        """
        Check if a given prefix is within a reserved prefix.
        """

        node = None
        if "." in prefix:
            node = self.__reserved_tree_ipv4.search_best(prefix)
        elif ":" in prefix:
            node = self.__reserved_tree_ipv6.search_best(prefix)

        return node is not None
