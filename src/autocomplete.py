from meilisearch.index import Index
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
import json

START = "<em>"
END = "</em>"

def _parse_highlighted_field(field, strict=False, first=False, minlen=None):
    def _weak_filter(x: str) -> bool:
        return START in x
    def _strong_filter(x: str) -> bool:
        return x.startswith(START) and x.endswith(START)
    def _minlen_filter(x: str) -> bool:
        success = _weak_filter(x)
        if success:
            ind = x.replace(START, "").find(END)
            success = (ind+1 >= minlen) or _strong_filter(x)
        return success         

    ## Set correct filter and apply
    f = _strong_filter if strict else ( _minlen_filter if minlen else _weak_filter)
    fields = field.split(",_,")
    fields = filter(f, fields)
    fields = map(
            lambda x: x.replace(START, "").replace(END, ""),
            fields)

    ## Return list or first item
    res = list(fields)
    if first:
        res = None if len(res) == 0 else res[0]
    return res

def _rm_advertiser(queryString: str, advertiser_name: str) -> str:
    x = queryString.lower().split(" ")
    substrs = []
    substrs.extend(x)
    if len(x) > 1:
        for i in range(len(x) - 1):
            substrs.append(" ".join(x[i:i+2]))
    res = process.extractOne(advertiser_name.lower(), substrs, scorer=fuzz.ratio)[0]
    return queryString.replace(res, "").replace("  ", " ").lstrip()


def searchSuggestions(args: dict, index: Index):
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

    advertiser_names = _parse_highlighted_field(first_hit['advertiser_names'])
    advertiser_names = { a: _rm_advertiser(searchString, a) for a in advertiser_names }

    data.update({
        "advertiser_names": advertiser_names,
        "color": _parse_highlighted_field(first_hit['colors'], minlen=3, first=True),
        "hits": list(map(_process_doc, hits))
    })
    return data