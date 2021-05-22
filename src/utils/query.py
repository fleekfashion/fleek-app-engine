import typing as t

import sqlalchemy as s
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.selectable import Alias, CTE, Select
from sqlalchemy import subquery 
from sqlalchemy import Column
from sqlalchemy.orm.session import Session
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func
from sqlalchemy.sql.expression import literal

DELIMITER = ",_,"

def _apply_filter(
        session: Session,
        products_subquery: t.Union[Alias, CTE], 
        key: str,
        value: t.Any,
    ) -> Query:

    q = session.query(products_subquery)
    subq = products_subquery

    if key == "min_price":
        q = q.filter(subq.c.product_sale_price >= int(value))
    elif key == "max_price":
        q = q.filter(subq.c.product_sale_price <= int(value))
    elif key == "advertiser_name" and len(str(value)) > 0:
        advertiser_names = value.split(DELIMITER)
        q = q.filter(subq.c.advertiser_name.in_(literal(advertiser_names)))
    elif key == "on_sale" and value:
        q = q.filter(subq.c.product_sale_price < subq.c.product_price)
    else:
        return products_subquery
    return q.cte(f"{key}_filter_applied")

def apply_filters(
        session: Session,
        products_subquery: t.Union[Alias, CTE],
        args: dict,
        active_only: bool
    ) -> Query:

    subq = products_subquery
    for key, value in args.items():
        subq = _apply_filter(session, subq, key, value)
    
    final_q = session.query(subq)
    if active_only:
        final_q = final_q.filter(subq.c.is_active == True)
    return final_q

def join_product_color_info(session: Session, products_subquery: t.Union[Alias, CTE], product_id_field: str = 'product_id') -> Query:
    join_field = products_subquery.c[product_id_field]

    ## Get the ids
    alt_color_ids = session.query(
        p.ProductColorOptions.product_id,
        p.ProductColorOptions.alternate_color_product_id,
    ).filter(
        p.ProductColorOptions.product_id.in_(session.query(join_field))
    ).cte('alt_color_ids_cte')

    ## Get basic alt color info
    alt_color_info = session.query(
        alt_color_ids.c.product_id,
        postgresql.array_agg(
            func.json_build_object(
                'alternate_color_product_id', alt_color_ids.c.alternate_color_product_id,
                'product_image_url', p.ProductInfo.product_image_url
            )
        ).label('product_color_options')
    ).filter(alt_color_ids.c.product_id == p.ProductInfo.product_id) \
    .filter(p.ProductInfo.is_active == True) \
    .group_by(alt_color_ids.c.product_id) \
    .cte('alt_color_info_cte')
    
    ## Join with original query
    final_query = session.query(
        products_subquery, 
        alt_color_info.c.product_color_options
    ).join(
        alt_color_info, 
        alt_color_info.c.product_id == join_field, 
        isouter=True
    )
    return final_query

def join_product_sizes(session: Session, products_subquery: t.Union[Alias, CTE], product_id_field: str = 'product_id') -> Query:
    join_field = products_subquery.c[product_id_field]

    sizes_subquery = session.query(
        p.ProductSizeInfo.product_id,
        postgresql.array_agg(
            func.json_build_object(
                'size', p.ProductSizeInfo.size,
                'product_purchase_url', p.ProductSizeInfo.product_purchase_url,
                'in_stock', p.ProductSizeInfo.in_stock,
            )
        ).label('sizes')
    ).filter(
        p.ProductSizeInfo.product_id.in_(session.query(join_field))
    ) \
     .group_by(p.ProductSizeInfo.product_id) \
     .cte('add_size_cte')

    return session.query(products_subquery, sizes_subquery.c.sizes) \
                  .join(sizes_subquery, sizes_subquery.c.product_id == join_field, isouter=True)

def join_product_info(
        session: Session, 
        subquery: t.Union[Alias, CTE], 
        product_id_field: str = 'product_id'
) -> Query:
    join_field = subquery.c[product_id_field]
    products_query = session.query(
            *[c for c in p.ProductInfo.__table__.c 
                if c.name not in subquery.c ],
            subquery
        ).filter(join_field == p.ProductInfo.product_id) \
        .cte('add_base_product_info_cte')

    color_info_products_subquery = join_product_color_info(
        session,
        products_query,
        product_id_field=product_id_field
    ).cte('products_add_color_info_cte')

    parsed_products_query = join_product_sizes(
        session, 
        color_info_products_subquery,
        product_id_field=product_id_field
    )

    return parsed_products_query
