import typing as t
import random

import sqlalchemy as s
from sqlalchemy.sql.selectable import Alias, CTE, Select
from sqlalchemy import subquery 
from sqlalchemy import Column
from sqlalchemy.orm.session import Session
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F, or_
from sqlalchemy.sql.expression import literal, literal_column
from sqlalchemy.dialects.postgresql import array

from src.utils import user_info
from src.utils import static 

DELIMITER = ",_,"

def gen_rand() -> str:
    return "_" + str(random.randint(1, 10**8))

def sort_columns(
        q: t.Union[Alias, CTE]
    ) -> Select:
    ordered_q = s.select(*[
        c.label(c.name) for c in sorted(q.c, key=lambda x: x.name)
    ])
    return ordered_q

    
def union_by_names(
        q1: t.Union[Alias, CTE], 
        q2: t.Union[Alias, CTE],
        union_all = False
    ) -> Select:

    ordered_q1 = sort_columns(q1)
    ordered_q2 = sort_columns(q2)

    res = ordered_q1.union_all(ordered_q2) if union_all \
        else ordered_q1.union(ordered_q2)
    return res

        
def apply_ranking(
        products_subquery: t.Union[Alias, CTE], 
        user_id: int, 
        pct: float,
    ) -> Select:
    def _get_scaling_factor(user_id: int, pct: float):
        n_advertisers = len(static.get_advertiser_names())
        n_fave_brands = len(user_info.get_user_fave_brands(user_id))
        boost_size = 2.0*pct*n_advertisers
        avg_boost = boost_size/max(n_fave_brands, 4) ## protect against overdoing 1 brand
        return 1.0/max(2, avg_boost)

    AC = p.AdvertiserProductCount
    scaling_factor = _get_scaling_factor(user_id, pct)
    
    faved_brands = s.select(
        p.UserFavedBrands.advertiser_name,
        literal(scaling_factor).label('scaling_factor')
    ).filter(p.UserFavedBrands.user_id == literal(user_id)) \
    .cte('fave_brands' + gen_rand())

    ranked_products = s.select(
        products_subquery,
        (F.random()*F.sqrt(AC.n_products) \
            *F.cbrt(AC.n_products) \
            *F.cbrt(F.sqrt(F.sqrt(AC.n_products))) \
            *F.coalesce(faved_brands.c.scaling_factor, 1)
        ).label('normalized_rank'),
    ).join(
        faved_brands,
        products_subquery.c.advertiser_name == faved_brands.c.advertiser_name,
        isouter=True
    ).filter(products_subquery.c.advertiser_name == AC.advertiser_name) \
    .cte('ranked_products' + gen_rand())

    final_q = s.select(ranked_products) \
                  .order_by(ranked_products.c.normalized_rank.asc())
    return final_q

def _apply_product_search_filter(
        products_subquery: t.Union[Alias, CTE],
        query: t.Any
    ) -> Select:

    product_search_words = query.rstrip().lstrip().split(' ')

    product_label_clauses = or_(*[F.array_to_string(products_subquery.c.product_labels, DELIMITER).ilike(f'%{word}%') for word in product_search_words])
    product_color_clauses = or_(products_subquery.c.color.ilike(word) for word in product_search_words)
    product_advertiser_name_clauses = or_(products_subquery.c.advertiser_name.ilike(f'%{word}%') for word in product_search_words)
    product_name_clauses = or_(products_subquery.c.product_name.ilike(f'%{word}%') for word in product_search_words)
    product_secondary_labels_clauses = or_(*[F.array_to_string(products_subquery.c.product_secondary_labels, DELIMITER).ilike(f'%{word}%') for word in product_search_words])
    product_internal_color_clauses = or_(products_subquery.c.internal_color.ilike(word) for word in product_search_words)
    product_brand_clauses = or_(products_subquery.c.product_brand.ilike(f'%{word}%') for word in product_search_words)
    product_tags_clauses = or_(*[F.array_to_string(products_subquery.c.product_tags, DELIMITER).ilike(f'%{word}%') for word in product_search_words])

    product_query_with_match_cols = s.select(
        products_subquery, 
        product_label_clauses.label('matches_product_labels'),
        product_color_clauses.label('matches_color'),
        product_advertiser_name_clauses.label('matches_advertiser_name'),
        product_name_clauses.label('matches_product_name'),
        product_secondary_labels_clauses.label('matches_product_secondary_labels'),
        product_internal_color_clauses.label('matches_internal_color'),
        product_brand_clauses.label('matches_product_brand'),
        product_tags_clauses.label('matches_product_tags')
    ).cte()

    match_cols = filter(lambda col: col.name.startswith('matches_'), product_query_with_match_cols.c)
    non_match_cols = filter(lambda col: not col.name.startswith('matches_'), product_query_with_match_cols.c)

    product_search_filter_query = s.select(
        non_match_cols
    ).filter(
        or_(
            *[col == True for col in match_cols]
        )
    ).order_by(
        *[col.desc() for col in match_cols]
    )

    return product_search_filter_query

def _apply_filter(
        products_subquery: t.Union[Alias, CTE], 
        key: str,
        value: t.Any,
    ) -> Select:

    q = s.select(products_subquery)
    subq = products_subquery

    if key == "min_price":
        q = q.filter(subq.c.product_sale_price >= int(value))
    elif key == "max_price":
        q = q.filter(subq.c.product_sale_price <= int(value))
    elif key == "advertiser_name" and len(str(value)) > 0:
        advertiser_names = value.split(DELIMITER) if type(value) == str else value
        q = q.filter(subq.c.advertiser_name.in_(literal(advertiser_names)))
    elif (key == "product_tag" or key == "product_labels") and len(str(value)) > 0:
        product_labels = value.split(DELIMITER) if type(value) == str else value
        q = q.filter(subq.c.product_labels.overlap(array(product_labels)) )
    elif key == "on_sale" and value:
        q = q.filter(subq.c.product_sale_price < subq.c.product_price)
    elif key == "product_search_string":
        q = _apply_product_search_filter(subq, value)
    else:
        return products_subquery
    return q.cte(f"{key}_filter_applied" + gen_rand())

def apply_filters(
        products_subquery: t.Union[Alias, CTE],
        args: dict,
        active_only: bool
    ) -> Select:

    subq = products_subquery
    for key, value in args.items():
        subq = _apply_filter(subq, key, value)
    
    final_q = s.select(subq)
    if active_only:
        final_q = final_q.filter(subq.c.is_active == True)
    return final_q

def join_product_color_info(products_subquery: t.Union[Alias, CTE], product_id_field: str = 'product_id') -> Select:
    join_field = products_subquery.c[product_id_field]

    ## Get the ids
    alt_color_ids = s.select(
        p.ProductColorOptions.product_id,
        p.ProductColorOptions.alternate_color_product_id,
    ).filter(
        p.ProductColorOptions.product_id.in_(s.select(join_field))
    ).cte('alt_color_ids_cte' + gen_rand())

    ## Get basic alt color info
    alt_color_info = s.select(
        alt_color_ids.c.product_id,
        postgresql.array_agg(
            F.json_build_object(
                'product_id', alt_color_ids.c.alternate_color_product_id,
                'product_image_url', p.ProductInfo.product_image_url,
                'color', p.ProductInfo.color,
            )
        ).label('product_color_options')
    ).filter(alt_color_ids.c.alternate_color_product_id == p.ProductInfo.product_id) \
    .filter(p.ProductInfo.is_active == True) \
    .group_by(alt_color_ids.c.product_id) \
    .cte('alt_color_info_cte' + gen_rand())
    
    ## Join with original query
    final_query = s.select(
        products_subquery, 
        alt_color_info.c.product_color_options
    ).join(
        alt_color_info, 
        alt_color_info.c.product_id == join_field, 
        isouter=True
    )
    return final_query

def join_product_sizes(products_subquery: t.Union[Alias, CTE], product_id_field: str = 'product_id') -> Select:
    join_field = products_subquery.c[product_id_field]

    sizes_subquery = s.select(
        p.ProductSizeInfo.product_id,
        postgresql.array_agg(
            F.json_build_object(
                'size', p.ProductSizeInfo.size,
                'product_purchase_url', p.ProductSizeInfo.product_purchase_url,
                'in_stock', p.ProductSizeInfo.in_stock,
            )
        ).label('sizes')
    ).filter(
        p.ProductSizeInfo.product_id.in_(s.select(join_field))
    ) \
     .group_by(p.ProductSizeInfo.product_id) \
     .cte('add_size_cte' + gen_rand())

    return s.select(products_subquery, sizes_subquery.c.sizes) \
                  .join(sizes_subquery, sizes_subquery.c.product_id == join_field, isouter=True)

def join_external_product_info(
        products_subquery: t.Union[Alias, CTE], 
        product_id_field: str = 'product_id'
    ) -> Select:

    color_info_products_subquery = join_product_color_info(
        products_subquery,
        product_id_field=product_id_field
    ).cte('products_add_color_info_cte' + gen_rand())

    parsed_products_query = join_product_sizes(
        color_info_products_subquery,
        product_id_field=product_id_field
    )
    return parsed_products_query

def join_base_product_info(
        subquery: t.Union[Alias, CTE],
        product_id_field: str = 'product_id'
    ) -> Select:
    join_field = subquery.c[product_id_field]

    ## Base Product Info
    products_query = s.select(
            *[c for c in p.ProductInfo.__table__.c 
                if c.name not in subquery.c ],
            subquery
        ).filter(join_field == p.ProductInfo.product_id)
    return products_query

def join_product_info(
        subquery: t.Union[Alias, CTE], 
        product_id_field: str = 'product_id'
    ) -> Select:

    ## Base product info
    products_query = join_base_product_info(
        subquery,
        product_id_field=product_id_field
    ).cte('base_product_info' + gen_rand())

    ## All external Product Info
    parsed_products_query = join_external_product_info(
        products_query,
        product_id_field=product_id_field
    )
    return parsed_products_query
