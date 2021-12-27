#  This software is Copyright (c) 2015 The Regents of the University of
#  California. All Rights Reserved. Permission to copy, modify, and distribute this
#  software and its documentation for academic research and education purposes, without fee, and without a written agreement is hereby granted, provided that
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

# Ported from dataconcierge's IXPInfo class.
# Authors: Alistair King, Chiara Orsini, Mingwei Zhang

import logging
import os
import re
import socket
import sqlite3
import sys
import tempfile
import time
import unicodedata
from urllib.request import urlretrieve, urlcleanup

import radix
import sqlalchemy
import wandio
from fuzzywuzzy import fuzz
from sqlalchemy.ext.declarative import as_declarative, declared_attr


@as_declarative()
class Base(object):
    """Base class which provides automated table name
    a surrogate primary key column, and a timestamp column.
    """
    data_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    file_path = sqlalchemy.Column(sqlalchemy.String, unique=True)
    timestamp = sqlalchemy.Column(sqlalchemy.Integer)

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower().replace("meta", "")


def load_settings():
    settings = {
        "db": {
            'drivername': 'postgresql',
            'host': 'localhost',
            'port': '5432',
            'username': 'caidadata',
            'password': '',
            'database': 'caidadata'
        },
        'file_path_prefix': '',
    }
    config_file = os.path.expanduser('~') + "/.caidadata_access"
    if os.path.isfile(config_file):
        with open(config_file) as f:
            for line in f:
                # avoid commented lines
                if line.startswith("#"):
                    continue
                fields = line.rstrip().split("=")
                if len(fields) == 2:
                    k = fields[0].lstrip().rstrip()
                    v = fields[1].lstrip().rstrip()
                    if k in settings["db"]:
                        settings["db"][k] = v
                    elif k in settings:
                        settings[k] = v
    return settings


class IXPInfoMeta(Base):
    """The IXP info class.
    This represents all IANA resource files in a single table.
    """
    source = sqlalchemy.Column(sqlalchemy.String)
    data_type = sqlalchemy.Column(sqlalchemy.String)

    tab_reg = {"peeringdb": {"sqlite": re.compile(r'^peeringdb_dump_(\d+_\d+_\d+)\.sqlite$'), },
               "pch": {"ip2asn": re.compile(r'^pch\.(\d+)\.ip2asn\.txt$'), },
               "euro-ix": {"ixp": re.compile(r'^ixp-service-matrix_(\d+)\.csv$'), },
               "merged": {"prefixes": re.compile(r'^ixp_prefixes_merged_pdb_pch_he\.(\d+)\.txt$'), },
               }


class DataConcierge(object):
    """ A class that populates and queries the CAIDA data
    metadata database.
    """

    def __init__(self, db_name=None, file_path_prefix=None, debug=False):
        """

        :param db_name: database options (e.g. sqlite:////home/me/meta.db), if no parameter is provided
        the program uses the default settings in settings.py, if a ~/.caidadata_access file is present,
        the default settings are overwritten.
        :param file_path_prefix: prefix to prepend to each file name, if no parameter is provided the
        program uses the default settings in settings.py, if a ~/.caidadata_access file is present,
        the default settings are overwritten.
        :param debug: activate debugging information
        """

        if debug:
            logging.basicConfig(stream=sys.stderr)
            logging.getLogger('sqlalchemy').setLevel(logging.DEBUG)

        # max number of attempts, and wait unit
        max_attempts = 3
        attempts = max_attempts
        wait = 10
        self.file_path_prefix = None
        # load settings
        settings = load_settings()
        if file_path_prefix:
            self.file_path_prefix = file_path_prefix
        else:
            self.file_path_prefix = settings["file_path_prefix"]

        # attempt to connect to the database
        while attempts > 0:
            # TODO: add some documentation/pointers here
            if db_name is None:
                self.db_name = "default"
                db_settings = settings["db"]
                self.engine = sqlalchemy.create_engine(sqlalchemy.engine.url.URL(**db_settings), echo=False)
            else:
                self.db_name = db_name
                self.engine = sqlalchemy.create_engine(self.db_name, echo=False)
            if not self.engine:
                sys.stderr.write("Can't create sqlalchemy engine, retrying!\n")
                time.sleep((max_attempts - attempts) * wait + 1)
                attempts -= 1
                continue
            # engine created
            self.session = None
            self.SessionCreate = None
            self.created = False
            self.meta = sqlalchemy.MetaData()
            try:
                self.meta.reflect(bind=self.engine)
            except sqlalchemy.exc.SQLAlchemyError:
                sys.stderr.write("Can't reflect database objects, retrying!\n")
                time.sleep((max_attempts - attempts) * wait + 1)
                attempts -= 1
                continue
            # engine binded to metadata
            self.db_mapping = {
                "ixpinfo": IXPInfoMeta,
            }
            # create db
            if not self._create_meta_db():
                sys.stderr.write("Can't connect to database, retrying!\n")
                time.sleep((max_attempts - attempts) * wait + 1)
                attempts -= 1
                continue
            # process ended correctly
            break
        # if the program is here and attempts == 0
        # then the connection did not succed, hence raise an exception
        if attempts == 0:
            raise

    def _create_meta_db(self):
        """ Create all the configured tables """
        if not self.created:
            # this is equivalent to the create table
            # statement in SQL
            Base.metadata.create_all(self.engine)
            self.SessionCreate = sqlalchemy.orm.sessionmaker(bind=self.engine)
            self.created = True
        return self.SessionCreate

    @staticmethod
    def _get_generic_meta(dtable, session, ts):

        # store in all_cols all the columns of the dtable
        # sote in group_cols all the columns that should affect the group by
        mapper = sqlalchemy.inspect(dtable)
        group_cols = []
        all_cols = []
        # collect all the cols except file_path and timestamp
        for column in list(mapper.columns):
            all_cols.append(dtable.__tablename__ + "." + str(column.key))
            if str(column.key) == "data_id" or \
                    str(column.key) == "file_path" or \
                    str(column.key) == "timestamp":
                continue
            else:
                group_cols.append(column.key)

        # query that selects one entry per group having the maximum
        # timestamp prior to the ts provided

        statement = " SELECT " + ", ".join(all_cols) + " "
        statement += " FROM " + dtable.__tablename__ + ",  ( "
        statement += " SELECT max(timestamp) as max_ts"
        if len(group_cols) > 0:
            statement += ", " + ", ".join(group_cols)
        statement += " FROM " + dtable.__tablename__ + " "
        statement += " WHERE timestamp <= " + str(ts) + " "
        if len(group_cols) > 0:
            statement += "GROUP BY " + ",".join(group_cols)
        statement += " ) max_ts_data"
        statement += " WHERE max_ts_data.max_ts = " + dtable.__tablename__ + ".timestamp "
        for col in group_cols:
            statement += " AND max_ts_data." + str(col) + " = "
            statement += dtable.__tablename__ + "." + str(col) + " "

        # execute the statement
        result = session.execute(statement)

        return result.fetchall()

    def get_dataset_meta(self, ds, ts):
        """ Returns the metadata for a specific dataset type having the largest timestamp prior
        to ts.

        :param ds: dataset name (string), supported types are 'asrank', 'asorg', 'dnsnames','arkwarts',ianaresources', and 'ixpinfo'
        :param ts: timestamp (integer)
        :return:  a list of dataset metadata information (each of them containing the file_path and timestamp field, along with specific dataset annotations)

        """
        db_results = []
        if ds not in self.db_mapping:
            logging.error("get_dataset_meta() - dataset not recognized")
            return db_results
        if ts < 0:
            logging.error("get_dataset_meta() - Invalid timestamp\n")
            return db_results
        dtable = self.db_mapping[ds]
        session = self.SessionCreate()

        db_results = self._get_generic_meta(dtable, session, ts)

        session.close()

        # a list of results (each of them in the form of a dictionary)
        ds_results = []

        for res in db_results:
            d = dict(list(res.items()))
            # if the request is over http, then we have to prepend
            # the right prefix
            if self.file_path_prefix and self.file_path_prefix != '':
                d['file_path'] = self.file_path_prefix + d['file_path']
            ds_results.append(d)

        return ds_results


def load_ixp_false_sim():
    # original map
    false_similar_ixp = {
        "MAD-IX": "MD-IX",
        "PTT-SP": "PTT-SJP",
        "PTT-SC": "PTT-SJC",
        "TP-IX": "TOP-IX",
        "OMSK-IX": "MSK-IX",
        "ADN-IX": "DN-IX",
        "UA-IX": "UAE-IX",
        "VRZ-IX": "RZ-IX",
        "MXP": "MIXP",
        "JPIX": "PIX",
        "SFIX": "SFMIX",
        "TWIX": "WIX",
        "BNIX": "BIX",
    }
    # adding the reverse
    full = dict()
    for name in false_similar_ixp:
        n1 = name.lower()
        n2 = false_similar_ixp[name].lower()
        full[n1] = n2
        full[n2] = n1
    # returning the complete dictionary
    return full


class IXPInfo(object):
    """IXPInfo provides methods to access the IXP datasets.

    :param ts: timestamp (to load the correct datasets)
    """

    def __init__(self, ts=None):
        self._data_con = DataConcierge()
        self._request_ts = None
        self._ixp_instances = None

        # associate an IXP id to each prefix
        self._prefix_ixp = radix.Radix()
        # ASn -> list of IXP ids
        self._asn_ixp_id = dict()
        # IXP ASn - id
        self._ixp_asn_id = dict()

        self.sim_threshold = 90
        self.false_sim = load_ixp_false_sim()

        # initialize the internal structures
        if ts:
            self.update_ts(ts)

    def _reset(self):
        self._request_ts = None
        self._ixp_id_info = dict()
        self._ixp_name_id = dict()
        self._prefix_ixp = radix.Radix()
        self._interface_participant = dict()
        self._asn_ixp_id = dict()
        self._ixp_asn_id = dict()
        self._ixp_rs_id = dict()

    @staticmethod
    def _is_default_route(pfx):
        (addr, mask) = pfx.split("/")
        if mask and int(mask) == 0:
            return True
        return False

    @staticmethod
    def _is_ip_address(pfx):
        address = pfx
        if "/" in pfx:
            address = pfx.split("/")[0]
        # check if ipv4
        try:
            socket.inet_aton(address)
            return True
        except socket.error:
            # not ipv4
            pass
        # check if ipv6
        try:
            socket.inet_pton(socket.AF_INET6, address)
            return True
        except socket.error:
            # not ipv6 either
            pass
        sys.stderr.write("WARNING: invalid IP address: " + pfx + "\n")
        return False

    def _best_match_ixp(self, ixp_name):
        ixp_name = ixp_name.replace("_", " ").lower()
        if ixp_name in self._ixp_name_id:
            return ixp_name, self._ixp_name_id[ixp_name]
        else:
            ixp_best = ""
            max_sim = 0
            for name in self._ixp_name_id:
                sim = fuzz.ratio(name, ixp_name)
                if sim > max_sim:
                    max_sim = sim
                    ixp_best = name
            if max_sim > 0:
                return ixp_best, self._ixp_name_id[ixp_best]
        return "", -1

    def _match_ixp(self, ixp_name):
        ixp_name = ixp_name.replace("_", " ").lower()
        if ixp_name in self._ixp_name_id:
            return self._ixp_name_id[ixp_name]
        else:
            for name in self._ixp_name_id:
                if fuzz.ratio(name, ixp_name) > self.sim_threshold:
                    # avoid false positives
                    if name in self.false_sim:
                        if self.false_sim[name] == ixp_name:
                            continue
                    # similar name inherit the same ixp id
                    return self._ixp_name_id[name]
        return -1

    def _add_ixp(self, ixp_name):
        ixp_name = ixp_name.replace("_", " ").lower()
        if ixp_name not in self._ixp_name_id:
            # check for IXPs with a very similar name
            for name in self._ixp_name_id:
                if fuzz.ratio(name, ixp_name) > self.sim_threshold:
                    # avoid false positives
                    if name in self.false_sim:
                        if self.false_sim[name] == ixp_name:
                            continue
                    # similar name inherit the same ixp id
                    return self._ixp_name_id[name]
            # if not, we add a new IXP
            new_id = len(self._ixp_id_info) + 1
            self._ixp_name_id[ixp_name] = new_id
            self._ixp_id_info[new_id] = {
                "name": ixp_name,
                "route-server": None,
                "ixp-asn": None,
                "participants": set(),
                "prefixes": set(),
            }
            return new_id
        else:
            return self._ixp_name_id[ixp_name]

    def _load_peeringdb_info(self, sqlite_db):
        """Link to peeringdb file
        :param sqlite_db:
        :return:
        """
        conn = sqlite3.connect(sqlite_db)
        c = conn.cursor()
        pdb_local_id = dict()
        # get all the IXP ids and names
        # SELECT id, name FROM mgmtPublics;
        c.execute("SELECT id, name FROM mgmtPublics")
        all_rows = c.fetchall()
        for (pdb_id, ixp_name) in all_rows:
            ixp_name = unicodedata.normalize("NFKD", ixp_name)
            local_id = self._add_ixp(ixp_name)
            pdb_local_id[pdb_id] = local_id

        # get all the prefixes associated with one
        # or more IXPs
        c.execute("SELECT address, public_id FROM mgmtPublicsIPs")
        all_rows = c.fetchall()
        for (pfx, pdb_id) in all_rows:
            if pdb_id not in pdb_local_id:
                continue
            pfx = pfx.split()[0]
            if "/" not in pfx:
                continue
            if self._is_default_route(pfx):
                continue
            # add a node to the patricia tree
            try:
                rnode = self._prefix_ixp.add(pfx)
            except ValueError:
                continue
            if rnode:
                if "ixps" not in rnode.data:
                    rnode.data["ixps"] = set()
                rnode.data["ixps"].add(pdb_local_id[pdb_id])
                self._ixp_id_info[pdb_local_id[pdb_id]]["prefixes"].add(pfx)

        # get the local peer ASN and the IXP associated with
        # a specific ip address
        c.execute(
            "SELECT local_ipaddr, public_id, local_asn FROM peerParticipantsPublics"
        )
        all_rows = c.fetchall()
        for (ip_addr, pdb_id, peer_asn) in all_rows:
            if pdb_id not in pdb_local_id:
                continue
            peer_asn = str(peer_asn)
            local_id = pdb_local_id[pdb_id]
            if peer_asn not in self._asn_ixp_id:
                self._asn_ixp_id[peer_asn] = set()
            self._asn_ixp_id[peer_asn].add(local_id)

        conn.close()

    def _load_merged_pfxs(self, file_path):
        file_handler = wandio.open(file_path)
        for line in file_handler:
            # ignore comments
            if line[0] == "#":
                continue
            fields = line.rstrip().split()
            if len(fields) != 2:
                continue
            pfx = fields[0]
            # ignore non-pfx and default routes
            if "/" not in pfx:
                continue
            if self._is_default_route(pfx):
                continue
            ixp_names = []
            # extracting names from the list
            raw_names = fields[1].split(",")
            for name in raw_names:
                if name.startswith("pdb_"):
                    ixp_names.append(name.replace("pdb_", ""))
                elif name.startswith("pch_"):
                    ixp_names.append(name.replace("pch_", ""))
                elif name.startswith("he_"):
                    ixp_names.append(name.replace("he_", ""))
                else:
                    # not a valid name, e.g. n/a
                    pass
            ixp_id = -1
            # first check if at least one of the names matches
            for name in ixp_names:
                ixp_id = self._match_ixp(name)
                if ixp_id != -1:
                    break
            # otherwise add a new IXP
            if ixp_id == -1 and len(ixp_names) > 0:
                ixp_id = self._add_ixp(ixp_names[0])

            if ixp_id != -1:
                # add a node to the patricia tree
                try:
                    rnode = self._prefix_ixp.add(pfx)
                except ValueError:
                    continue
                if rnode:
                    if "ixps" not in rnode.data:
                        rnode.data["ixps"] = set()
                    rnode.data["ixps"].add(ixp_id)
                    self._ixp_id_info[ixp_id]["prefixes"].add(pfx)
        file_handler.close()

    def _retrieve_data(self):
        """return the list of ixp datasets that are available
        on the caida data database, whose timestamp is the closest
        to ts (and antecedent)
        """
        for data in self._ixp_instances:
            if data["data_type"] == "sqlite":
                # get the path to the sqlite db
                # sqlite cannot be read when on a network
                # mounted partition, so we always copy the file
                # in a temporary directory
                tmp_dir = tempfile.mkdtemp()  # (prefix="./")
                # download the sqlite db in the tmp dir
                urlretrieve(data["file_path"], tmp_dir + "/ixp.db")
                sqlite_file = tmp_dir + "/ixp.db"
                # load in memory the database and extract the information needed
                if data["source"] == "peeringdb":
                    self._load_peeringdb_info(sqlite_file)
                # Clean up the tmp directory
                os.remove(sqlite_file)
                urlcleanup()
                os.removedirs(tmp_dir)
            if data["source"] == "merged" and data["data_type"] == "prefixes":
                self._load_merged_pfxs(data["file_path"])

    def update_ts(self, ts):
        """(Re)load the IXPInfo dataset which is the
        most suitable for the timestamp provided, i.e.
        the one with the greatest timestamp prior to ts.
        :param ts: timestamp
        """
        if self._request_ts != ts:
            ret_ixp_instances = self._data_con.get_dataset_meta("ixpinfo", ts)
            # if the returned instances differ from the previous
            # ones
            if self._ixp_instances != ret_ixp_instances:
                self._ixp_instances = ret_ixp_instances
                self._reset()
                if self._ixp_instances:
                    self._retrieve_data()
            self._request_ts = ts
        if len(self._ixp_id_info) == 0:
            sys.stderr.write("WARNING: IXP data are not available\n")

    def get_ixp_prefix_match(self, pfx):
        """Checks if the prefix is associated with an IXP lan and returns the IXP id.
        :param pfx: lan prefix (string)
        :return: the set of IXP ids associated with the lan prefix
        :rtype: set
        """
        # longest prefix match
        if self._prefix_ixp:
            if self._is_ip_address(pfx):
                rnode = self._prefix_ixp.search_best(pfx)
                if rnode:
                    return list(rnode.data["ixps"])
        return None

    def get_common_ixps(self, asn1, asn2):
        """Returns the list of IXPs that simultaneously have ASn1 and ASn2 as participants.
        :param asn1: first AS number (string)
        :param asn2: second AS number (string)
        :return: a list of IXP ids
        """
        if self._asn_ixp_id:
            common = []
            if str(asn1) in self._asn_ixp_id:
                asn1_ixps = self._asn_ixp_id[str(asn1)]
                if str(asn2) in self._asn_ixp_id:
                    asn2_ixps = self._asn_ixp_id[str(asn2)]
                    for ixp_id in asn1_ixps:
                        if ixp_id in asn2_ixps:
                            common.append(ixp_id)
            return common
        return None


if __name__ == '__main__':
    print("lalala")
    ixp = IXPInfo()
    ixp.update_ts(1577836800)
    if ixp.get_ixp_prefix_match("80.249.208.246"):
        print("matched prefix")
    if ixp.get_ixp_prefix_match("80.249.208.247/32"):
        print("matched prefix")
