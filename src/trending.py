import copy
import json
import re
from typing import Dict, List, Any

from functional import seq 
from meilisearch.index import Index

def _load_meili_results(searchString: str, args: Dict[str, str], index: Index) -> Dict[Any, Any]:
    query_args = {
            "limit": int(args.get('limit', 50)),
            "offset": int(args.get('offset', 0)),
    }
    data = index.search(query=searchString, opt_params=query_args)
    return data

def trendingSearches(args: dict, index: Index) -> Dict:
    searchString  = ""
    data = _load_meili_results(searchString, args, index)
    return data
