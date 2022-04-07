import datetime
import logging

import wandio

from grip import common


def get_pfx_origins_path(basedir, timestamp):
    # year=2015/month=01/day=06/hour=09/pfx-origins.1420536000.gz
    timestamp = int(timestamp)
    time = datetime.datetime.utcfromtimestamp(timestamp)

    path = basedir + "/pfx-origins/year={}/month={:02}/day={:02}/hour={:02}/pfx-origins.{}.gz".format(
        time.year, time.month, time.day, time.hour, timestamp
    )
    return path


def load_pfx_file(basedir, timestamp):
    path = get_pfx_origins_path(basedir, timestamp)
    pfx2as_dict = {}

    logging.info("pfx_origins.py: Loading pfx2as mappings into memory from %s" % path)
    try:
        with wandio.open(path) as fh:
            for line in fh:
                # 1476104400|115.116.0.0/16|4755|4755|STABLE
                ts, prefix, old_asn, new_asn, label = line.strip().split("|")

                if label == "REMOVED" or ":" in prefix:
                    # do not insert prefixes that are no longer announced
                    # we also do not (currently) support IPv6 prefixes
                    continue

                ases = []
                for asnstr in new_asn.split(" "):
                    if "{" in asnstr:
                        ases.extend(asnstr.strip("{}").split(","))
                    else:
                        ases.append(asnstr)

                pfx2as_dict[prefix] = ases

    except IOError as e:
        logging.error("Could not read pfx-origin file '%s'" % path)
        logging.error("I/O error: %s" % e.strerror)
        return None
    except ValueError as e:
        logging.error(e.args)
        return None
    logging.info("...loading pfx2as mappings finished")
    return pfx2as_dict


# swift://bgp-hijacks-pfx-origins/year=2019/month=12/day=18/hour=19/pfx-origins.1576698900.gz
# swift://bgp-hijacks-pfx-origins/year=2019/month=12/day=18/hour=19/pfx-origins.1576698900.gz

if __name__ == "__main__":
    logging.basicConfig(level="INFO",
                        format="%(asctime)s|%(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    load_pfx_file(1420536000)
