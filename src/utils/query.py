import typing as t
import random
from datetime import datetime, timedelta

import sqlalchemy as s
from sqlalchemy.sql import Values
from sqlalchemy.sql.dml import Insert, Update
from sqlalchemy.sql.elements import UnaryExpression
from sqlalchemy.sql.selectable import Alias, CTE, Select
from sqlalchemy import subquery 
from sqlalchemy import Column
from sqlalchemy.orm.session import Session
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F, or_
from sqlalchemy.sql.expression import literal, literal_column
from sqlalchemy.dialects.postgresql import array
from werkzeug.datastructures import ImmutableMultiDict
import src.utils.sqlalchemy_utils as sqa

from src.utils import user_info
from src.utils import static 
DELIMITER = ",_,"

def days_ago_timestamp(days: int) -> int:
    return int((datetime.utcnow() - timedelta(days=days) ).timestamp())

def get_daily_random_seed() -> float:
    random.seed(datetime.utcnow().date())
    return random.random()

def sort_columns(q: t.Union[Alias, CTE]) -> Select:
    ordered_q = s.select(*[
        c.label(c.name) for c in sorted(q.c, key=lambda x: x.name)
    ])
    return ordered_q

def sort_product_preview(products: t.List[dict]) -> t.List[dict]:
    return sorted(
            products, 
            key=lambda x: ( x['last_modified_timestamp'], x['product_id'] ),
            reverse=True
    )
    
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
    .cte()

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
    .cte()

    final_q = s.select(ranked_products) \
                  .order_by(ranked_products.c.normalized_rank.asc())
    return final_q

def parse_filters(args: t.Union[dict, ImmutableMultiDict]) -> dict:
    if isinstance(args, ImmutableMultiDict):
        res = { 
            **args,
            'advertiser_names': args.getlist('advertiser_names'),
            'product_labels': args.getlist('product_labels'),
            'product_secondary_labels': args.getlist('product_secondary_labels')
        }
    else:
        res = args
    res2 = { key: value for key, value in res.items() if not isinstance(value, list) or len(value) > 0 }
    return res2

def _apply_product_search_filter(
        products_subquery: t.Union[Alias, CTE],
        query: t.Any
    ) -> Select:

    product_search_words = query.replace('-', ' ').lower().rstrip().lstrip().split(' ')

    word_match_cols = []
    for word in product_search_words:
        col_match_clauses = [
            products_subquery.c.product_labels.overlap(array([word])),
            products_subquery.c.color.op('~*')(f'\m{word}\M'),
            products_subquery.c.advertiser_name.op('~*')(f'\m{word}\M'),
            products_subquery.c.product_name.op('~*')(f'\m{word}\M'),
            products_subquery.c.product_secondary_labels.overlap(array([word])),
            products_subquery.c.internal_color.op('~*')(f'\m{word}\M'),
            products_subquery.c.product_tags.overlap(array([word])),
        ]
        word_match_col = or_(*[col == True for col in col_match_clauses])
        word_match_cols.append(word_match_col)

    col_prefix = "matches"
    product_query_with_match_cols = s.select(
        products_subquery,
        *[col.label(f"{col_prefix}_${index}") for index, col in enumerate(word_match_cols)]
    ).cte()

    match_cols = filter(lambda col: col.name.startswith(col_prefix), product_query_with_match_cols.c)
    non_match_cols = filter(lambda col: not col.name.startswith(col_prefix), product_query_with_match_cols.c)

    # print(sqa.run_query(s.select(match_cols)))

    product_search_filter_query = s.select(
        non_match_cols
    )
    for col in match_cols: product_search_filter_query = product_search_filter_query.where(col == True)

    return product_search_filter_query

def _apply_filter(
        q: Select,
        products_subquery: t.Union[Alias, CTE],
        key: str,
        value: t.Any,
    ) -> Select:

    subq = products_subquery

    if key == "min_price":
        q = q.filter(subq.c.product_sale_price >= int(value))
    elif key == "max_price":
        q = q.filter(subq.c.product_sale_price <= int(value))
    elif key in ["advertiser_name", "advertiser_names"] and len(str(value)) > 0:
        advertiser_names = value.split(DELIMITER) if type(value) == str else value
        q = q.filter(subq.c.advertiser_name.in_(literal(advertiser_names)))
    elif (key == "product_tag" or key == "product_labels") and len(str(value)) > 0:
        product_labels = value.split(DELIMITER) if type(value) == str else value
        q = q.filter(subq.c.product_labels.overlap(array(product_labels)) )
    elif key == "on_sale" and value:
        q = q.filter(subq.c.product_sale_price < subq.c.product_price)
    elif key == "product_search_string":
        q = _apply_product_search_filter(q.cte(), value)
    return q

def apply_filters(
        products_subquery: t.Union[Alias, CTE],
        args: dict,
        active_only: bool
    ) -> Select:

    filters = parse_filters(args)
    q = s.select(products_subquery)
    q = q.filter(products_subquery.c.is_active == True) if active_only \
        else q

    for key, value in filters.items():
        q = _apply_filter(q, products_subquery, key, value)
    return q

def insert_on_filter_condition(args: dict, table: p.PostgreTable, filter_condition: UnaryExpression) -> Insert:
    cols = table.__table__.c
    values = Values(*[s.column(c.name, c.type) for c in cols], name="temp_" + table.__table__.name)
    data = tuple([args[c.name] for c in cols])
    
    data_on_filter_statement = s.select(values.data([data])) \
        .where(filter_condition) \
        .cte()
    
    insert_statement = s.insert(table).from_select(
        [col.name for col in data_on_filter_statement.c],
        data_on_filter_statement
    )
    return insert_statement

def join_product_color_info(products_subquery: t.Union[Alias, CTE], product_id_field: str = 'product_id') -> Select:
    join_field = products_subquery.c[product_id_field]

    ## Get the ids
    alt_color_ids = s.select(
        p.ProductColorOptions.product_id,
        p.ProductColorOptions.alternate_color_product_id,
    ).filter(
        p.ProductColorOptions.product_id.in_(s.select(join_field))
    ).cte()

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
    .cte()
    
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
     .cte()

    return s.select(products_subquery, sizes_subquery.c.sizes) \
                  .join(sizes_subquery, sizes_subquery.c.product_id == join_field, isouter=True)

def join_external_product_info(
        products_subquery: t.Union[Alias, CTE], 
        product_id_field: str = 'product_id'
    ) -> Select:

    color_info_products_subquery = join_product_color_info(
        products_subquery,
        product_id_field=product_id_field
    ).cte()

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
    ).cte()

    ## All external Product Info
    parsed_products_query = join_external_product_info(
        products_query,
        product_id_field=product_id_field
    )
    return parsed_products_query

def _get_user_board_products(
        user_id: int, 
        offset: int, 
        limit: int, 
        n_products: int) -> Select:
    user_board_ids_subq = s.select(p.UserBoard.board_id, p.UserBoard.user_id) \
        .filter(p.UserBoard.user_id == user_id) \
        .order_by(p.UserBoard.last_modified_timestamp.desc()) \
        .offset(offset) \
        .limit(limit) \
        .cte()

    board_product_lateral_subq = s.select(
            p.BoardProduct.board_id, 
            p.BoardProduct.product_id
    ) \
        .filter(p.BoardProduct.board_id == user_board_ids_subq.c.board_id) \
        .order_by(p.BoardProduct.last_modified_timestamp.desc(), p.BoardProduct.product_id) \
        .limit(n_products) \
        .subquery() \
        .lateral()

    return s.select(
            board_product_lateral_subq, 
            user_board_ids_subq.c.user_id
        ).join(board_product_lateral_subq, s.true())

def join_board_info(q: CTE) -> Select:
    return s.select(
            q,
            p.Board.name, 
            p.Board.creation_date, 
            p.Board.description, 
            p.Board.artwork_url,
            p.Board.board_type,
            p.Board.last_modified_timestamp
        ) \
        .join(q, q.c.board_id == p.Board.board_id)
