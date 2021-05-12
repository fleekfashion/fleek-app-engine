import copy
import json
import re
from typing import Dict, List, Any

from cachetools import LRUCache, cached, ttl, TTLCache

import cachetools.func
from functional import seq 
from meilisearch.index import Index
from src.productSearch import build_filters
from functional import pseq 

SUGGESTION_IMAGE_CACHE: TTLCache = TTLCache(maxsize=2**10, ttl=60*60)

def _load_meili_results(
        searchString: str, 
        args: Dict[str, Any],
        index: Index) -> Dict[Any, Any]:
    data = index.search(query=searchString, opt_params=args)
    return data

def get_first_image_url(
        params: Dict[str, Any],
        index: Index
    ) -> str:
    searchString = params.get('suggestion', '')
    l = params.get('product_label')
    labels = [l] if l else None
    filters = build_filters(
            advertiser_names=None,
            product_labels=labels,
            product_secondary_labels=None,
            internal_colors=None,
            max_price=10*6,
            min_price=0
    )

    query_args = {
            "limit": 1,
            **filters
    }
    res = _load_meili_results(searchString, query_args, index)
    image_url = res['hits'][0]['product_image_url']
    return image_url

def update_image_urls(data: list, index: Index) -> List[dict]:
    res = pseq(data).map(lambda d:
        {
            **d, 
            "product_image_url": get_first_image_url(d, index)
        }
    ).to_list()

    return res

def trendingSearches(args: dict, index: Index, product_index: Index) -> Dict:
    KEY = 'trending'
    if KEY in SUGGESTION_IMAGE_CACHE:
        print('success')
        return SUGGESTION_IMAGE_CACHE[KEY] 

    query_args = {
            "limit": int(args.get('limit', 50)),
            "offset": int(args.get('offset', 0)),
    }
    searchString  = ""
    data = _load_meili_results(searchString, query_args, index)
    data['hits'] = update_image_urls(data['hits'], product_index)

    SUGGESTION_IMAGE_CACHE[KEY] = data
    return data

def labelSearches(args: dict, index: Index, product_index: Index) -> Dict:
    KEY = 'labels'
    if KEY in SUGGESTION_IMAGE_CACHE:
        print('success')
        return SUGGESTION_IMAGE_CACHE[KEY] 

    query_args = {
            "limit": int(args.get('limit', 50)),
            "offset": int(args.get('offset', 0)),
    }
    searchString  = ""
    data = _load_meili_results(searchString, query_args, index)
    data['hits'] = update_image_urls(data['hits'], product_index)

    SUGGESTION_IMAGE_CACHE[KEY] = data
    return data
