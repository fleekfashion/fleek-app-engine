from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import Session
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func

def join_product_sizes(session: Session, query: Query) -> Query:
    products_subquery = query.subquery(reduce_columns=True)
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
        p.ProductSizeInfo.product_id.in_(session.query(products_subquery.c.product_id))
    ) \
     .group_by(p.ProductSizeInfo.product_id) \
     .subquery()

    return session.query(products_subquery, sizes_subquery.c.sizes) \
                  .join(sizes_subquery, sizes_subquery.c.product_id == products_subquery.c.product_id, isouter=True)

def join_product_info(session: Session, product_id_query: Query) -> Query:
    subquery = product_id_query.subquery(reduce_columns=True)
    products_query = session.query(
            p.ProductInfo,
            subquery
        ).filter(subquery.c.product_id == p.ProductInfo.product_id)
    parsed_products_query = join_product_sizes(session, products_query)
    return parsed_products_query