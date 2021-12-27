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

"""
Frequently used ElasticSearch queries generator
"""


def query_out_of_range(start_ts, end_ts):
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "position": "NEW"
                        }
                    }
                ],
                "must_not": {
                    "range": {
                        "view_ts": {
                            "lte": end_ts,
                            "gte": start_ts
                        }
                    }
                }
            }
        }
    }
    return query


def query_in_range(start_ts, end_ts,
                   inserted_before=None, inserted_after=None, modified_before=None, modified_after=None,
                   must_tags=None, must_not_tags=None, missing_inference=False, missing_data=False,
                   must_tr_worthy=False, max_susp=None, min_susp=None, size=1000):

    view_ts_range = {"range": {"view_ts": {}}}
    if start_ts:
        view_ts_range["range"]["view_ts"]["gte"] = start_ts
    if end_ts:
        view_ts_range["range"]["view_ts"]["lte"] = end_ts

    insert_ts_range = {"range": {"insert_ts": {}}}
    if inserted_before:
        insert_ts_range["range"]["insert_ts"]["lte"] = inserted_before
    if inserted_after:
        insert_ts_range["range"]["insert_ts"]["gte"] = inserted_after

    modified_ts_range = {"range": {"last_modified_ts": {}}}
    if modified_before:
        modified_ts_range["range"]["last_modified_ts"]["lte"] = modified_before
    if modified_after:
        modified_ts_range["range"]["last_modified_ts"]["gte"] = modified_after

    suspicion_level_range = {"range": {"summary.inference_result.primary_inference.suspicion_level": {}}}
    if max_susp:
        suspicion_level_range["range"]["summary.inference_result.primary_inference.suspicion_level"]["lte"] = max_susp
    if min_susp:
        suspicion_level_range["range"]["summary.inference_result.primary_inference.suspicion_level"]["gte"] = min_susp

    query = {
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "position": "NEW"
                        }
                    },
                    view_ts_range
                ],
                "must_not": [

                ]
            }
        },
        "sort": {
            "view_ts": {
                "order": "asc"
            }
        }
    }

    if must_tr_worthy:
        query["query"]["bool"]["must"].append({
            "match": {
                "summary.tr_worthy": "true"
            }
        })

    if inserted_before or inserted_after:
        query["query"]["bool"]["must"].append(insert_ts_range)
    if modified_after or modified_before:
        query["query"]["bool"]["must"].append(modified_ts_range)

    if must_tags:
        query["query"]["bool"]["must"].append({
            "terms": {
                "summary.tags": must_tags
            }
        })

    if must_not_tags:
        query["query"]["bool"]["must_not"].append({
            "terms": {
                "summary.tags": must_not_tags
            }
        })
        query["query"]["bool"]["must_not"].append({
            "terms": {
                "summary.tags.name": must_not_tags
            }
        })

    if missing_inference:
        query["query"]["bool"]["must_not"].append({
            "exists": {
                "field": "summary.inference_result.inferences"
            }
        })

    if missing_data:
        query["query"]["bool"]["must_not"].append({
            "exists": {
                "field": "asinfo"
            }
        })

    return query


def query_unfinished_events(start_ts=None, end_ts=None, source_fields=None):
    query = {
        "size": 1000,
        "from": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "position": "NEW"
                        }
                    }
                ],
                "must_not": {
                    "exists": {
                        "field": "finished_ts"
                    }
                }
            }
        },
        "sort": {
            "view_ts": {
                "order": "asc"
            }
        }
    }
    filter_query = None
    if start_ts is not None or end_ts is not None:
        filter_query = {
            "range": {
                "view_ts": {
                }
            }
        }
        if start_ts is not None:
            filter_query["range"]["view_ts"]["gte"] = start_ts
        if end_ts is not None:
            filter_query["range"]["view_ts"]["lte"] = end_ts

    if filter_query is not None:
        query["query"]["bool"]["filter"] = filter_query

    if source_fields is not None:
        assert (isinstance(source_fields, list))
        query["_source"] = source_fields

    return query


def query_closest_finished_event(pfxevent):
    event_type = pfxevent.event_type
    view_ts = pfxevent.view_ts

    query = {
        "from": 0,
        "query": {
            "bool": {
                "filter": {
                    "range": {
                        "view_ts": {
                            "gte": view_ts
                        }
                    }
                },
                "must": [
                    {
                        "match": {
                            "position": "FINISHED"
                        }
                    }
                ]
            }
        },
        "size": 1,
        "sort": {
            "view_ts": {
                "order": "asc"
            }
        }
    }

    if event_type == "moas" or event_type == "edges":
        prefix = pfxevent.details.get_prefix_of_interest()
        query['query']['bool']['must'].append({
            "term": {
                "pfx_events.details.prefix": prefix
            }
        })
    else:
        super_pfx, sub_pfx = pfxevent.details.get_prefixes()
        query['query']['bool']['must'].append({
            "term": {
                "pfx_events.details.super_pfx": super_pfx
            }
        })
        query['query']['bool']['must'].append({
            "term": {
                "pfx_events.details.sub_pfx": sub_pfx
            }
        })

    return query


def query_by_tags(tags):
    tag_terms = [
        {"term": {"tags": tag}}
        for tag in tags
    ]
    query = {
        "size": 100,
        "query": {
            "bool": {
                "should": tag_terms,
                "minimum_should_match": 1
            }
        },
        "sort": [{
            "view_ts": {"order": "desc"}
        }]
    }

    return query


def query_missing_traceroutes(min_ts=None):
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "pfx_events.traceroutes.worthy": "true"
                        }
                    }
                ],
                "must_not": [
                    {
                        "exists": {
                            "field": "pfx_events.traceroutes.msms.results"
                        }
                    }
                ]
            }
        },
        "sort": [{
            "view_ts": {"order": "desc"}
        }]
    }

    if min_ts:
        query["query"]["bool"]["filter"] = {
            "range": {
                "view_ts": {
                    "gte": min_ts
                }
            }
        }

    return query


def query_asns_on_spamhaus_list(asns, ts):
    """
    Query to check if ASN is on the Spamhaus ASNDROP list.
    :param asns: AS numbers
    :param ts: timestamp
    :return:
    """
    assert (isinstance(asns, list))
    assert (isinstance(ts, int))
    should_lst = []

    for asn in asns:
        should_lst.append(
            {
                "match": {
                    "data.asn": asn
                }
            }
        )
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "expires": {
                                "format": "epoch_second",
                                "gte": ts
                            }
                        }
                    },
                    {
                        "range": {
                            "last_modified": {
                                "format": "epoch_second",
                                "lte": ts
                            }
                        }
                    }
                ],
                "should": should_lst,
                "minimum_should_match": 1
            }
        }
    }

    return query


def query_spamhaus_list(ts):
    """
    Get the most recent Spamhaus ASNDROP list for the specified timestamp.
    :param ts: timestamp
    :return:
    """
    assert (isinstance(ts, int))

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "last_modified": {
                                "format": "epoch_second",
                                "lte": ts
                            }
                        }
                    }
                ]
            }
        },
        "sort": {
            "last_modified": {
                "order": "desc"
            }
        },
        "size": 1
    }

    return query


def query_no_inference(max_ts=None):
    """
    Query for events that have no inference results
    :param max_ts: Maximum timestamp to search to
    :return:
    """
    query = {
        "query": {
            "bool": {
                "must": {
                    "range": {
                        "view_ts": {
                            "format": "epoch_second",
                            "lte": max_ts
                        }
                    }
                },
                "must_not": {
                    "exists": {
                        "field": "inference"
                    }
                }
            }
        },
        "sort": {
            "view_ts": {
                "order": "desc"
            }
        },
        "size": 1
    }

    return query
