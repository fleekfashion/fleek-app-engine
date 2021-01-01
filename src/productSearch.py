from meilisearch.index import Index
import json

def _build_facet_filters(
        advertiser_names: list,
        product_labels: list,
        ) -> list:
    f = []
    if advertiser_names:
        f.append([ f"advertiser_name:{name}"
                for name in advertiser_names ])
    if product_labels:
        f.append([ f"product_labels:{label}"
                for label in product_labels])
    if len(f) == 0:
        f = None
    return f

def _build_value_filters(
        min_price: int,
        max_price: int
        ) -> str:
    return f"product_sale_price >= {min_price} AND product_sale_price <= {max_price}"

def build_filters(
        advertiser_names: list,
        product_labels: list,
        min_price: int,
        max_price: int,
    ) -> dict:
    filters = {
        "facetFilters": _build_facet_filters(advertiser_names, product_labels),
        "filters": _build_value_filters(min_price, max_price)
    }
    return filters

def productSearch(args, index: Index) -> list:
    searchString  = args['searchString'].rstrip().lstrip()
    advertiser_names = args.getlist("advertiser_names")
    product_labels = args.getlist("product_labels")
    max_price = args.get("max_price", 10000)
    min_price = args.get("min_price", 0)

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
        max_price=max_price,
        min_price=min_price
        )
    )
    data = index.search(query=searchString, opt_params=query_args)
    return data
