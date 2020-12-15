from meilisearch.index import Index
import json

def searchSuggestions(args: dict, index: Index):

    def _parse_highlighted_field(field, strict=False, first=False):
        def _weak_filter(x: str) -> bool:
            return "<em>" in x
        def _strong_filter(x: str) -> bool:
            return x.startswith("<em>") and x.endswith("</em>")
        f = _strong_filter if strict else _weak_filter
        
        fields = field.split(",_,")
        fields = filter(f, fields)
        fields = map(
                lambda x: x.replace("<em>", "").replace("</em>", ""),
                fields)

        res = list(fields)
        if first:
            res = None if len(res) == 0 else res[0]
        return res

    def _process_doc(doc: dict):
        doc.pop("_formatted")
        doc.pop("advertiser_names")
        doc.pop("colors")
        return doc

    searchString  = args['searchString']
    query_args = {
            "limit": int(args.get('limit', 6)),
            "offset": int(args.get('offset', 0)),
            "attributesToHighlight": ["advertiser_names", "colors"]
    }
    data = index.search(query=searchString, opt_params=query_args)
    hits = data['hits']
    if len(hits) == 0:
        return data
    first_hit = hits[0]['_formatted']

    data.update({
        "advertiser_names": _parse_highlighted_field(first_hit['advertiser_names']),
        "color": _parse_highlighted_field(first_hit['colors'], strict=True, first=True),
        "hits": list(map(_process_doc, hits))
    })
    return data
