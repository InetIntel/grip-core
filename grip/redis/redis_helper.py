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

import calendar
import datetime
import logging
import socket
import struct

import redis


class RedisHelper:

    DEFAULT_HOST = "giglio.cc.gatech.edu"
    # change the default host to local host to avoid accidentally modify data on other machines
    # DEFAULT_HOST = "localhost"

    def __init__(self, host=DEFAULT_HOST, port="6379", db=0, log_level="INFO"):
        self.host = host
        self.port = port
        self.db = db
        self.red = None
        self.pipe = None

        self._init_logging(log_level)
        self.connect()

    @staticmethod
    def _init_logging(loglevel):
        logging.basicConfig(level=loglevel,
                            format="%(asctime)s|%(levelname)s: %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
        
    @staticmethod
    def version():
        print("1.0")

    def connect(self):
        self.red = redis.StrictRedis(host=self.host, port=self.port, db=self.db, decode_responses=True)
        self._set_pipeline()

    def _set_pipeline(self):
        if self.pipe is None:
            self.pipe = self.red.pipeline(transaction=False)

    def get_pipeline(self):
        self._set_pipeline()
        return self.pipe

    def scan_keys(self, key):
        return self.red.scan_iter(key)

    def __getattr__(self, attr):
        return getattr(self.red, attr)

    @staticmethod
    def datestr_to_epoch(date):
        # return the midnight time
        return calendar.timegm(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple())

    @staticmethod
    def get_bin_pfx(pfx):
        ip, mask = pfx.split("/")
        try:
            bin_ip = socket.inet_aton(ip)
        except socket.error:
            logging.warning("malformatted IP address: {}".format(ip))
            return None
        (int_ip,) = struct.unpack("!L", bin_ip)
        return format(int_ip, "032b")[:int(mask)]

    @staticmethod
    def get_str_pfx(bin_pfx):
        mask = len(bin_pfx)
        # pad the IP to 32 bit
        bin_ip = bin_pfx.ljust(32, '0')
        # find the octets, convert to int, join them, append the mask
        str_ip = ".".join([str(int(bin_ip[off:off+8], 2))
                           for off in [0, 8, 16, 24]])
        return "%s/%d" % (str_ip, mask)
