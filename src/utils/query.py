import typing as t

from sqlalchemy.orm.query import Query
from sqlalchemy.sql.selectable import Alias, CTE
from sqlalchemy import subquery 
from sqlalchemy import Column
from sqlalchemy.orm.session import Session
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func

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

    parsed_products_query = join_product_sizes(
            session, 
            products_query, 
            product_id_field=product_id_field
        )
    return parsed_products_query
