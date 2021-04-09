import typing as t
import re

from meilisearch.index import Index
from functional import seq
import json
from src.defs.utils import HIDDEN_LABEL_FIELDS
from src.utils.user_info import get_user_fave_brands
from src.utils.static import get_advertiser_counts
from src.utils.hashers import apple_id_to_user_id_hash

MIN_SEARCH_TAG_HITS = 5
MAX_SEARCH_TAG_BRANDS = 2

def _build_facet_filters(
        advertiser_names: t.Optional[t.List[str]],
        product_labels: t.Optional[t.List[str]],
        product_secondary_labels: t.Optional[t.List[str]],
        internal_colors: t.Optional[t.List[str]],
        ) -> t.Optional[t.List[t.List[str]]]:
    f = []
    if advertiser_names:
        f.append([ f"advertiser_name:{name}"
                for name in advertiser_names ])
    if product_labels:
        f.append([ f"product_labels:{label}"
                for label in product_labels])
    if product_secondary_labels:
        f.append([ f"product_secondary_labels:{label}"
                for label in product_secondary_labels])
    if internal_colors:
        f.append([ f"internal_colors:{color}"
                for color in internal_colors])
    if len(f) == 0:
        return None
    return f

def _build_value_filters(
        min_price: int,
        max_price: int
        ) -> str:
    return f"product_sale_price >= {min_price} AND product_sale_price <= {max_price}"

def build_filters(
        advertiser_names: t.Optional[t.List[str]],
        product_labels: t.Optional[t.List[str]],
        product_secondary_labels: t.Optional[t.List[str]],
        internal_colors: t.Optional[t.List[str]],
        min_price: int,
        max_price: int,
    ) -> dict:
    filters = {
        "facetFilters": _build_facet_filters(
            advertiser_names, 
            product_labels, 
            product_secondary_labels, 
            internal_colors),
        "filters": _build_value_filters(min_price, max_price)
    }
    return filters



def process_facets_distributions(searchString: str, facets_distr: dict, product_label_filter_applied: bool, advertiser_filter_applied: bool, user_id: t.Optional[int]) -> t.List[t.Dict[str, str]]:
    def _build_suggestion(searchString: str, name: str, filter_type: str) -> str:
        suggestion = ""
        if filter_type == "product_labels":
            suggestion = f"{searchString} {name}"
        elif filter_type == "product_secondary_labels":
            if name in HIDDEN_LABEL_FIELDS.keys():
                bad_label = HIDDEN_LABEL_FIELDS[name]
                searchString = re.sub(f"\\b{bad_label}\\b", '', searchString).rstrip().lstrip()
                suggestion = f"{searchString} {name}"
            else:
                suggestion = f"{name} {searchString}"
        elif filter_type == "internal_color":
            suggestion = f"{name} {searchString}"
        suggestion = re.sub('\s+',' ', suggestion).rstrip().lstrip()
        return suggestion
            

    fave_brands = get_user_fave_brands(user_id) if user_id else []
    advertiser_counts = get_advertiser_counts() 
    print(fave_brands)

    res = []
    advertiser_tags = {}
    for key, value in facets_distr.items():
        if key == "advertiser_name":
            if advertiser_filter_applied:
                continue
            normalized_brands = sorted([ 
                    (nbHits/advertiser_counts.get(name, 10**10), nbHits, name ) 
                    for name, nbHits in value.items() 
                    if nbHits > MIN_SEARCH_TAG_HITS and name in fave_brands
            ])[:MAX_SEARCH_TAG_BRANDS]
            tags = [
                {
                    "suggestion": searchString,
                    "filter_type": key,
                    "nbHits": brand[1],
                    "filter": brand[2] 
                }
                for brand in normalized_brands
            ]
            for i in range(len(tags)):
                advertiser_tags[i] = tags[i]
        elif key == "product_labels" and product_label_filter_applied:
            continue
        elif key == "product_tags":
            continue
        else:
            for name, nbHits in value.items():
                res.append(
                    {
                        "suggestion": _build_suggestion(searchString, name, key),
                        "filter_type": key,
                        "nbHits": nbHits,
                        "filter": name
                    }
                )
    processed_res = seq(sorted(res, key=lambda x: x['nbHits'], reverse=True)) \
        .filter(lambda x: x['nbHits'] > MIN_SEARCH_TAG_HITS) \
        .filter(lambda x: x['filter'] not in searchString) \
        .take(10) \
        .to_list()
    
    def _get_item_as_list(key, d: dict):
        return [d[key]] if key in d else []

    final_res = processed_res[:2] + _get_item_as_list(0, advertiser_tags) + processed_res[2:6] + _get_item_as_list(0, advertiser_tags) + processed_res[6:]
    return final_res 

def productSearch(args, index: Index) -> list:
    searchString  = args['searchString'].rstrip().lstrip()
    advertiser_names = args.getlist("advertiser_names")
    product_labels = args.getlist("product_labels")
    product_secondary_labels = args.getlist("product_secondary_labels")
    internal_colors = args.getlist("internal_colors")
    max_price = args.get("max_price", 10000)
    min_price = args.get("min_price", 0)
    user_id = apple_id_to_user_id_hash(args.get("user_id", None))

    query_args = {
            "offset": int(args.get('offset', 0)),
            "limit": int(args.get('limit', 10)),
            "facetsDistribution": [
                "product_labels",
                "product_secondary_labels",
                "product_tags",
                "advertiser_name",
                "internal_color"
                ]
    }
    query_args.update(build_filters(
        advertiser_names=advertiser_names,
        product_labels=product_labels,
        product_secondary_labels=product_secondary_labels,
        internal_colors=internal_colors,
        max_price=max_price,
        min_price=min_price
        )
    )
    data = index.search(query=searchString, opt_params=query_args)
    data['search_tags'] = process_facets_distributions(
        searchString=searchString,
        facets_distr=data['facetsDistribution'], 
        product_label_filter_applied=len(product_labels)>0,
        advertiser_filter_applied=len(advertiser_names)>0,
        user_id=user_id
    )
    return data
