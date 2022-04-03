# TODO ADD NEW COPYRIGHT

import os
from datetime import datetime
from glob import glob
from itertools import chain

def fs_generate_file_list(basepath):

    files = (chain.from_iterable(glob(os.path.join(x[0], '*.gz'))
            for x in os.walk(basepath)))

    return files

def fs_get_consumer_filename_from_ts(basedir, event_type, ts):
    ts = int(ts)
    datestr = ""

    # TODO uncomment this if "live" consumer output is put into a
    # date-based directory hierarchy
    #datestr = datetime.utcfromtimestamp(ts).strftime("year=%Y/month=%m/day=%d/hour=%H")

    file_prefix = event_type

    if event_type == "submoas" or event_type == "defcon":
        file_prefix = "subpfx-" + file_prefix

    filename = "%s/%s/production/%s/%s.%s.events.gz" % \
            (basedir, event_type, datestr, file_prefix, ts)
    return filename

def fs_get_timestamp_from_file_path(fpath):
    return int(fpath.split("/")[-1].split(".")[1])
