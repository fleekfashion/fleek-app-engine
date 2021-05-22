import typing as t

from sqlalchemy.orm.query import Query
from sqlalchemy.sql.selectable import Alias, CTE
from sqlalchemy import subquery 
from sqlalchemy import Column
from sqlalchemy.orm.session import Session
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func

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
