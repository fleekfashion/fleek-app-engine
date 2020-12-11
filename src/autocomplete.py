from meilisearch.index import Index
import json

def searchSuggestions(args: dict, index: Index):
    searchString  = args['searchString']
    query_args = {
            "limit": int(args.get('limit', 6)),
            "offset": int(args.get('offset', 0)),
            "attributesToHighlight": ["advertiser_names"]
    }
    def _process_doc(doc: dict):
        advertisers = doc["_formatted"]["advertiser_names"].split("~~")
        advertisers = filter(lambda x: "<em>" in x, advertisers)
        advertisers = map(lambda x: x.replace("<em>", "").replace("</em>", ""),
                advertisers)
        doc['advertiser_names'] = list(advertisers)
        doc.pop("_formatted")
        return doc
    data = index.search(query=searchString, opt_params=query_args)
    hits = map(_process_doc, data['hits'])
    data['hits'] = list(hits)
    print(data)
    return data
