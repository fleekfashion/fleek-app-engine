import typing as t

from meilisearch.index import Index
from functional import seq
import json

def _build_facet_filters(
        advertiser_names: t.Optional[t.List[str]],
        product_labels: t.Optional[t.List[str]],
        product_secondary_labels: t.Optional[t.List[str]],
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
        min_price: int,
        max_price: int,
    ) -> dict:
    filters = {
        "facetFilters": _build_facet_filters(advertiser_names, product_labels, product_secondary_labels),
        "filters": _build_value_filters(min_price, max_price)
    }
    return filters



def process_facets_distributions(searchString: str, facets_distr: dict, product_label_filter_applied: bool) -> t.List[t.Dict[str, str]]:
    res = []
    for key, value in facets_distr.items():
        if key == "advertiser_name":
            continue
        if key == "product_labels" and product_label_filter_applied:
            continue

        for name, nbHits in value.items():
            res.append(
                {
                    "suggestion": f"{searchString} {name}",
                    "filter_type": key,
                    "nbHits": nbHits
                }
            )
    processed_res = seq(sorted(res, key=lambda x: x['nbHits'], reverse=True)) \
        .filter(lambda x: x['nbHits'] > 3) \
        .take(10) \
        .to_list()
    return processed_res

def productSearch(args, index: Index) -> list:
    searchString  = args['searchString'].rstrip().lstrip()
    advertiser_names = args.getlist("advertiser_names")
    product_labels = args.getlist("product_labels")
    product_secondary_labels = args.getlist("product_secondary_labels")
    max_price = args.get("max_price", 10000)
    min_price = args.get("min_price", 0)

    print("wowsa")
    query_args = {
            "offset": int(args.get('offset', 0)),
            "limit": int(args.get('limit', 10)),
            "facetsDistribution": [
                "product_labels",
                "product_secondary_labels",
                "product_tags",
                "advertiser_name"
                ]
    }
    query_args.update(build_filters(
        advertiser_names=advertiser_names,
        product_labels=product_labels,
        product_secondary_labels=product_secondary_labels,
        max_price=max_price,
        min_price=min_price
        )
    )
    data = index.search(query=searchString, opt_params=query_args)
    data['search_tags'] = process_facets_distributions(
        searchString=searchString,
        facets_distr=data['facetsDistribution'], 
        product_label_filter_applied=len(product_labels)>0
    )
    return data
