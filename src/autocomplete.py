from __future__ import annotations
import copy
import json
import re
from typing import Dict, List, Any
from dataclasses import dataclass

import cachetools.func

from functional import seq
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
from meilisearch.index import Index
from src.defs.utils import HIDDEN_LABEL_FIELDS

START = "<em>"
END = "</em>"
DISPLAY_FIELDS = ["product_label", "primary_attribute", "secondary_attribute", "attribute_descriptor", "colors"]


def _rm_tags(x):
    return x.replace(START, "").replace(END, "")

def _has_tags(x):
    return START in x or END in x

def _handle_spaces(x):
    return re.sub('\s+',' ', x).rstrip().lstrip()

class SuggestionWord:
    def __init__(self, text: str, pos: int, is_label: bool):
        self.text = text
        self.pos = pos
        self.is_label = is_label 
        self.is_bold = _has_tags(text)

    def __str__(self):
        return self.text


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


def _load_meili_results(searchString: str, offset: int, limit: int, index: Index) -> Dict[Any, Any]:
    print('no-cache')
    query_args = {
            "limit": limit,
            "offset": offset, 
            "attributesToHighlight": ["*"]
    }
    data = index.search(query=searchString, opt_params=query_args)
    hits = seq(data['hits']).map(lambda x: x['_formatted']).to_list()
    return { **data, 'hits': hits }

def _process_hits(hits: List[Dict[Any, Any]], searchString: str) -> Dict[Any, Any]:
    def _process_doc(doc: dict):
        doc.pop("advertiser_names")
        doc.pop("colors")
        return doc

    def _process_suggestion(x):
        suggestion = seq([
            SuggestionWord(
                text=word,
                pos=pos,
                is_label=word==x['product_label'] or word in HIDDEN_LABEL_FIELDS.keys()
            )
            for pos, word in enumerate(x['suggestion'].split())
            ]).sorted(key=lambda x: (x.is_label, x.is_bold, x.pos)) \
            .make_string(" ")
        return {**x, 'suggestion': suggestion}

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
                        .map(_process_doc) \
                        .map(_process_suggestion) \
                        .to_list()
    }


@cachetools.func.ttl_cache(ttl=6*60*60, maxsize=2**12)
def runSearch(searchString: str, offset: int, limit: int, index: Index) -> dict:
    data = _load_meili_results(searchString, offset, limit, index)
    processed_hits = _process_hits(data['hits'], searchString)
    n_hits = len(processed_hits['hits'])

    def _set_field(x, field, value):
        d = copy.copy(x)
        d[field] = value
        return d
    ## If no search results returned
    if seq(processed_hits.values()).for_all(lambda x: len(x) == 0):
        data = _load_meili_results(searchString.split()[-1], offset, offset+1, index)
        processed_hits = _process_hits(data['hits'], searchString)
        processed_hits['hits'] = []
        processed_hits['color'] = ""

    ## If you hit a super specific query, show alternative secondary_attributes
    first_hit = processed_hits['hits'][0] if n_hits > 0 else {}
    first_hit_label = first_hit.get('product_label')

    valid_hits = processed_hits['hits']

    all_suggestions = seq(
                processed_hits['hits']
            ).map(lambda x: x['suggestion_hash']) \
            .to_set()

    if len(valid_hits) < 3 and _rm_tags(first_hit.get('suggestion', '')) == searchString:
        ## Remove the secondary attribute from string
        searchStringTail = seq(searchString.split(" ")[1:]) \
                .map(_rm_tags) \
                .make_string(" ") \


        ## Load new results
        data2 = _load_meili_results(searchStringTail, offset, limit, index)
        new_hits= seq(
                _process_hits(data2['hits'], searchString)['hits']
            ).filter(lambda x: x['suggestion_hash'] not in all_suggestions) \
            .filter(lambda x: _rm_tags(x['suggestion']) != searchStringTail) \
            .filter(lambda x: x['product_label'] == first_hit_label or first_hit_label is None) \
            .take(limit - len(valid_hits))
        valid_hits.extend(new_hits)

    ## replace original hits
    processed_hits['hits'] = valid_hits

    processed_hits['hits'] = processed_hits['hits'][:limit]
    data.update(processed_hits)
    return data

def searchSuggestions(args: dict, index: Index) -> Dict:
    searchString  = args['searchString'].rstrip().lstrip()
    searchPrefix = None
    OFFSET = int(args.get('offset', 0))
    LIMIT = int(args.get('limit', 6))
    return runSearch(searchString, OFFSET, LIMIT, index) 
