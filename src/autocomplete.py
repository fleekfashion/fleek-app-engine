import copy
import json
import re
from typing import Dict, List, Any

from functional import seq 
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
from meilisearch.index import Index

START = "<em>"
END = "</em>"
DISPLAY_FIELDS = ["product_label", "primary_attribute", "secondary_attribute", "attribute_descriptor", "colors"]


def _rm_tags(x):
    return x.replace(START, "").replace(END, "")

def _parse_highlighted_field(field, strict=False, first=False, minlen=None, rm_tag=True):
    def _weak_filter(x: str) -> bool:
        return START in x
    def _strong_filter(x: str) -> bool:
        return x.startswith(START) and x.endswith(START)
    def _minlen_filter(x: str) -> bool:
        success = _weak_filter(x)
        if success:
            ind = x.replace(START, "").find(END)
            success = (ind >= minlen) or _strong_filter(x)
        return success

    ## Set correct filter and apply
    f = _strong_filter if strict else ( _minlen_filter if minlen else _weak_filter)
    fields = field.split(",_,")
    fields = filter(f, fields)
    fields = map(
            lambda x: _rm_tags(x) if rm_tag else x,
            fields)

    ## Return list or first item
    res = list(fields)
    if first:
        res = "" if len(res) == 0 else res[0]
    return res

def _rm_advertiser(queryString: str, advertiser_name: str) -> str:
    x = queryString.split(" ")
    substrs = []
    substrs.extend(x)
    if len(substrs) > 1:
        for i in range(len(x) - 1):
            substrs.append(" ".join(x[i:i+2]))

    ## Get scores
    ## Tiebreaker -> longest string
    res = process.extract(advertiser_name.lower(), substrs, scorer=fuzz.WRatio, limit=5)
    res = sorted(res, key=lambda x: (x[1], len(x[0])), reverse=True)[0][0]

    return re.sub(f"\\b{res}\\b", "", queryString) \
            .replace("  ", " ") \
            .lstrip()


def _load_meili_results(searchString: str, args: Dict[str, str], index: Index) -> Dict[Any, Any]:
    query_args = {
            "limit": int(args.get('limit', 6)),
            "offset": int(args.get('offset', 0)),
            "attributesToHighlight": ["*"]
    }
    data = index.search(query=searchString, opt_params=query_args)
    data['hits'] = seq(data['hits']).map(lambda x: x['_formatted']).to_list()
    return data

def _process_hits(hits: List[Dict[Any, Any]], searchString: str) -> Dict[Any, Any]:
    def _process_doc(doc: dict):
        doc.pop("advertiser_names")
        doc.pop("colors")
        return doc
    def _contains_display_match(hit):
        for f in DISPLAY_FIELDS:
            if START in hit.get(f, ""):
                return True
        return False
    def _get_advertiser_names(hit: Dict[str, Any]) -> Dict[str, str]:
        res = seq(_parse_highlighted_field(hit['advertiser_names'])) \
            .map(lambda x: (x, _rm_advertiser(searchString, x))) \
            .to_dict()
        return res

    if len(hits) == 0:
        return {"hits": []}

    return {
        "advertiser_names": _get_advertiser_names(hits[0]),
        "color": _parse_highlighted_field(hits[0]['colors'], minlen=3, first=True, rm_tag=False),
        "hits": seq(hits) \
                        .filter(_contains_display_match) \
                        .map(_process_doc) \
                        .to_list()
    }


def searchSuggestions(args: dict, index: Index) -> Dict:
    searchString  = args['searchString'].rstrip().lstrip()
    searchPrefix = None
    data = _load_meili_results(searchString, args, index)
    processed_hits = _process_hits(data['hits'], searchString)

    def _set_field(x, field, value):
        d = copy.copy(x)
        d[field] = value 
        return d
    ## If no search results returned
    if seq(processed_hits.values()).for_all(lambda x: len(x) == 0):
        searchPrefix = START + " ".join(searchString.split(" ")[:-1]) + END
        searchStringTail = searchString.split(" ")[-1]
        data = _load_meili_results(searchStringTail, args, index)
        processed_hits = _process_hits(data['hits'], searchString)
        processed_hits['hits'] = []
        processed_hits['color'] = ""

    ## If you hit a super specific query, show alternative secondary_attributes
    elif len(processed_hits['hits']) == 1:
        first_hit = processed_hits['hits'][0]
        suggestion = first_hit['suggestion']
        if  _rm_tags(suggestion) == searchString:
            searchStringTail = searchString \
                    .replace(_rm_tags(first_hit.get('secondary_attribute')), "") \
                    .replace("  ", "") \
                    .lstrip()
            data = _load_meili_results(searchStringTail, args, index)
            new_processed_hits = _process_hits(data['hits'], searchString)
            new_processed_hits['hits'] = seq(new_processed_hits['hits']) \
                .filter(lambda x: _rm_tags(x['suggestion']) != _rm_tags(suggestion)) \
                .filter(lambda x: len(x['secondary_attribute']) > 0) \
                .to_list()
            processed_hits['hits'].extend(new_processed_hits['hits'])
    processed_hits['hits'] = processed_hits['hits'][:args.get('limit', 6)]
    data.update(processed_hits)
    return data
