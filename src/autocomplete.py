from meilisearch.index import Index
import json

def searchSuggestions(args: dict, index: Index):
    def _get_advertiser_names(advertisers):
        advertisers = advertisers.split(",_,")
        advertisers = filter(
                lambda x: "<em>" in x,
                advertisers)
        advertisers = map(
                lambda x: x.replace("<em>", "").replace("</em>", ""),
                advertisers)
        return list(advertisers)
    def _process_doc(doc: dict):
        doc.pop("_formatted")
        doc.pop("advertiser_names")
        return doc

    searchString  = args['searchString']
    query_args = {
            "limit": int(args.get('limit', 6)),
            "offset": int(args.get('offset', 0)),
            "attributesToHighlight": ["advertiser_names"]
    }
    data = index.search(query=searchString, opt_params=query_args)
    hits = data['hits']
    if len(hits) == 0:
        return data

    data.update({
        "advertiser_names": _get_advertiser_names(hits[0]['_formatted']['advertiser_names']),
        "hits": list(map(_process_doc, hits))
    })
    return data
