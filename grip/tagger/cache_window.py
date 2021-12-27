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


class CacheWindow:

    def __init__(self, window_size=86400):
        self.event_time_dict = {}  # pfx_event to time mapping
        self.time_events_dict = {}  # time to pfx_events mapping
        self.window_size = window_size
        self.last_updated_ts = 0

    def __cleanup_cache(self, current_view_ts):
        """
        update the cache based on the current time stamp, remove old ones from cache.

        after this function call, all cached pfx event in self.event_time_dict should have been
        seen within the past window. and time_events_dict does not have data from older than
        one window before
        """

        if self.last_updated_ts == current_view_ts:
            # have updated the cache already for the current time stamp
            return

        to_be_removed_events = set()
        tmp_ts = set()
        for time_ts in self.time_events_dict:
            if current_view_ts - time_ts > self.window_size:
                # if the cached data is older than one day, clean it up
                # pop time from the time-events cache, and remove all events later
                # this helps to narrow down what events to remove
                tmp_ts.add(time_ts)
                to_be_removed_events.update(self.time_events_dict[time_ts])
        for ts in tmp_ts:
            logging.info("CacheWindow: pop outdated timestamp {}".format(ts))
            self.time_events_dict.pop(ts)

        for fingerprint in to_be_removed_events:
            # remove all old events from cache
            try:
                event_last_seen_ts = self.event_time_dict[fingerprint]
                if current_view_ts - event_last_seen_ts > self.window_size:
                    # if we have not seen the prefix event in the last WINDOW time
                    # we should remove this event from the cache
                    self.event_time_dict.pop(fingerprint)
            except KeyError as e:
                # fingerprint not found. shouldn't happen
                logging.error("CacheWindow: __update_cache: fingerprint not in event cache but in time cache: %s",
                              fingerprint)
                raise e

        # update last_update ts
        self.last_updated_ts = current_view_ts

    def is_old_event_and_update(self, pfx_event, show_warning=True):
        """
        check if an event seen before,
        and update the cache if necessary
        """

        current_view_ts = pfx_event.view_ts
        fingerprint = pfx_event.get_recurring_fingerprint()

        # clean up old cache entries
        self.__cleanup_cache(pfx_event.view_ts)

        if fingerprint in self.event_time_dict:
            # the prefix event has been seen in the past window
            last_seen_ts = self.event_time_dict[fingerprint]
            if pfx_event.event_type == "edges" and last_seen_ts == current_view_ts:
                # for edges events, excluding events with same fingerprint of the current timestamp.
                # it's ok to have multiple prefix events with the same as1_as2 as the fingerprint
                pass
            else:
                if show_warning:
                    logging.info("recurring prefix event {}. last seen at {} ({} seconds ago)".format(
                        fingerprint, last_seen_ts, current_view_ts - last_seen_ts
                    ))
                return True

        # this event has not been seen in the past
        # update the event's last seen time
        self.event_time_dict[fingerprint] = current_view_ts

        # add events to the view time's dictionary
        if current_view_ts not in self.time_events_dict:
            self.time_events_dict[current_view_ts] = set()
        self.time_events_dict[current_view_ts].add(fingerprint)

        return False
