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

import collections
import json
import logging

from future.utils import iteritems

from grip.events.event_summary import EventSummary
from grip.events.pfxevent import PfxEvent
from grip.metrics.event_metrics import EventMetrics
from grip.metrics.traceroute_metrics import TracerouteMetrics
from grip.utils.general import to_dict, parse_ts

MAX_PFX_EVENTS = 1000  # only output AS path info for the first 1k pfx events


class Event:
    """
    Event definition.
    """

    def __init__(
            self,
            # core fields
            event_type,
            position,  # NEW or FINISHED
            event_id,
            pfx_events=None,

            # timestamps
            view_ts=None,
            finished_ts=None,
            insert_ts=None,
            last_modified_ts=None,

            # dicts
            tr_metrics=None,
            event_metrics=None,

            asinfo=None,   # information related to specific autonomous systems
            debug=None,
    ):
        # basic
        self.event_type = event_type
        self.pfx_events = pfx_events if pfx_events is not None else []
        self.position = position
        self.event_id = event_id

        # parse timestamps
        self.view_ts = parse_ts(view_ts)
        self.finished_ts = parse_ts(finished_ts)
        self.insert_ts = parse_ts(insert_ts)
        self.last_modified_ts = parse_ts(last_modified_ts)

        # data
        if asinfo is None:
            asinfo = {}
        self.asinfo = asinfo

        # traceroute metrics
        if tr_metrics is None:
            tr_metrics = TracerouteMetrics()
        if isinstance(tr_metrics, dict):
            tr_metrics = TracerouteMetrics.from_dict(tr_metrics)
        assert isinstance(tr_metrics, TracerouteMetrics)
        self.tr_metrics = tr_metrics

        # event metrics
        if event_metrics is None:
            event_metrics = EventMetrics()
        if isinstance(event_metrics, dict):
            event_metrics = EventMetrics.from_dict(event_metrics)
        assert isinstance(event_metrics, EventMetrics)
        self.event_metrics = event_metrics

        if debug is None:
            self.debug = {}
        else:
            self.debug = debug

        self.summary = EventSummary(self)

    def add_pfx_event(self, pfx_event):
        """
        Add one PfxEvent object to the event.
        """
        if pfx_event.event_type != self.event_type or \
                pfx_event.view_ts != self.view_ts:
            raise ValueError("Prefix event (%s) is incompatible with Event (%s)" %
                             (pfx_event,
                              json.dumps({
                                  "event_type": self.event_type,
                                  "view_ts": self.view_ts,
                              })))
        # update summary stats
        self.pfx_events.append(pfx_event)

    def set_pfx_events(self, pfx_events):
        """
        Set the events PfxEvent objects.
        """
        for pfx_event in pfx_events:
            self.add_pfx_event(pfx_event)
        self.summary.update()

    def as_dict(self):
        """
        Return the Event object as a dict.
        """
        new_events = [e for e in self.pfx_events if e.position == "NEW"]

        incl_paths = True
        if len(new_events) > MAX_PFX_EVENTS:
            logging.warning("Event has too many prefix events (%d > %d). AS paths will be dropped." %
                            (len(self.pfx_events), MAX_PFX_EVENTS))
            incl_paths = False
        pfx_event_dicts = [pfx_event.as_dict(incl_paths=incl_paths) for pfx_event in self.pfx_events]

        # calculate event duration if finished_ts is set
        duration = self.finished_ts - self.view_ts if self.finished_ts else None

        self.event_metrics.update(self)
        self.tr_metrics.update_tags(self.pfx_events)

        return {
            # event information
            "id": self.event_id,
            "event_type": self.event_type,
            "position": self.position,

            # pfx events embeded objects
            "pfx_events": pfx_event_dicts,

            "view_ts": self.view_ts,
            "finished_ts": self.finished_ts,
            "insert_ts": self.insert_ts,
            "last_modified_ts": self.last_modified_ts,

            # metrics and summary
            "tr_metrics": to_dict(self.tr_metrics),
            "event_metrics": to_dict(self.event_metrics),
            "summary": to_dict(self.summary),
            "duration": duration,

            "asinfo": self.asinfo,
            "debug": self.debug,
        }

    @staticmethod
    def from_pfxevent(pe):
        """
        Extract a Event object from a PfxEvent object.
        """
        event = Event(
            event_type=pe.event_type,
            position=pe.position,
            view_ts=pe.view_ts,
            event_id=pe.get_event_id()
        )
        return event

    @staticmethod
    def from_dict(d):
        """
        Extract a Event object from a dictionary.
        """

        # TODO: what's the purpose of this?
        def convert(data):
            if isinstance(data, str):
                return str(data)
            elif isinstance(data, collections.Mapping):
                return dict(map(convert, iteritems(data)))
            elif isinstance(data, collections.Iterable):
                # FIXME: warning
                return type(data)(map(convert, data))
            else:
                return data

        # d = convert(d)

        pfx_events = d.pop("pfx_events")
        if "id" in d:
            d["event_id"] = d.pop("id")

        try:
            d.pop("duration")
            d.pop("summary")  # pop summary field, we always re-calculate summary based on the existing information
            d.pop("external")  # legacy field
        except KeyError:
            pass

        event = Event(**d)
        common = {
            "event_type": event.event_type,
            "view_ts": event.view_ts,
            "position": event.position
        }
        for ped in pfx_events:
            ped.update(common)
            pe = PfxEvent.from_dict(ped)
            # do a direct append to avoid double-updating stats
            event.add_pfx_event(pe)
        event.summary.update()
        return event

    def as_json(self):
        """
        Convert the Event object to a single-line JSON string.
        """
        d = self.as_dict()
        return json.dumps(d)

    def has_inference(self, inference_id: str):
        """
        Examine if event has certain inference, determined by inference id
        :param inference_id:
        :return:
        """

        return any(i.inference_id == inference_id for i in self.summary.inference_result.inferences)

    def add_to_asinfo(self, asn, field, value, override=True):
        """
        Add information about a given ASN.

        :param asn: autonomous system to update
        :param field: field name to update
        :param value: field value to update
        :param override: True if to override existing data
        :return:
        """
        asn = int(asn)
        if asn not in self.asinfo:
            self.asinfo[asn] = {}

        if not override and field in self.asinfo[asn]:
            # if we don't want to override data, and data exist, just return
            return
        self.asinfo[asn][field] = value
