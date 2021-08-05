import typing as t
import random
import itertools

import sqlalchemy as s
from sqlalchemy.sql.dml import Insert, Update
from sqlalchemy.sql.selectable import Alias, CTE, Select
from sqlalchemy import subquery 
from sqlalchemy import Column
from sqlalchemy.orm.session import Session
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy import func as F
from sqlalchemy.sql.expression import literal, literal_column
from sqlalchemy.dialects.postgresql import array
from werkzeug.datastructures import ImmutableMultiDict

from src.utils import query as qutils 

from src.utils import user_info
from src.utils import static 

def get_update_board_timestamp_stmt_from_select(board_ids: Select, timestamp: int) -> Update:
    return s.update(p.Board) \
        .where(p.Board.board_id.in_(board_ids)) \
        .values(last_modified_timestamp=timestamp) \
        .execution_options(synchronize_session=False)

def get_board_update_timestamp_statement(board_id: int, last_modified_timestamp: int) -> Update:
    return s.update(p.Board) \
        .where(p.Board.board_id == board_id) \
        .values(last_modified_timestamp=last_modified_timestamp)

def get_product_group_stats(
        products: CTE,
        id_col: t.Optional[str]
    ) -> Select:

    ## Process id col
    id_colname = id_col if id_col else "temp_id"
    tmp_id_col = literal_column(id_col) if id_col else literal(1)

    ## join pinfo
    q3 = qutils.join_base_product_info(products).cte()

    ## Get advertiser level stats
    advertiser_stats = s.select(
        tmp_id_col,
        q3.c.advertiser_name,
        F.count(q3.c.product_id).label('n_products')
    ).group_by(tmp_id_col, q3.c.advertiser_name) \
        .cte()

    ## Get total group level stats
    full_advertiser_stats = s.select(
        tmp_id_col,
        F.sum(advertiser_stats.c.n_products).label('n_products'),
        psql.array_agg(
            F.json_build_object(
                'advertiser_name', advertiser_stats.c.advertiser_name,
                'n_products', advertiser_stats.c.n_products
            )
        ).label('advertiser_stats'),
    ) \
        .group_by(tmp_id_col) \
        .cte()

    active_stats = s.select(
        tmp_id_col,
        F.sum(q3.c.product_price - q3.c.product_sale_price).label('total_savings')
    ) \
        .where(q3.c.is_active) \
        .group_by(tmp_id_col) \
        .cte()

    board_stats = s.select(
        full_advertiser_stats,
        active_stats.c.total_savings
    ).outerjoin(
        active_stats, 
        full_advertiser_stats.c[id_colname] == active_stats.c[id_colname]
    )

    return board_stats

def get_product_previews(
    products: CTE,
    id_col: str,
    order_field: str,
    desc: bool = True
    ) -> Select:

    tmp_id_col = literal_column(id_col)
    t = literal_column(order_field)
    order_by_field = t.desc() if desc else t

    ## Get row numbers
    board_products = s.select(
            tmp_id_col.label('tmp_id_col'),
            products.c.product_id,
            products.c.last_modified_timestamp,
            F.row_number() \
                .over(
                    tmp_id_col,
                    order_by=(
                        order_by_field,
                        products.c.product_id.desc()
                    )
                ).label('row_number')
        ) \
        .cte()

    ## Filter, join and get preview
    filtered_board_product = s.select(board_products) \
        .where(board_products.c.row_number <= 6)  \
        .cte()
    board_product_info = qutils.join_product_info(filtered_board_product).cte()

    ## Get preview
    product_previews = s.select(
            board_product_info.c.tmp_id_col.label(id_col),
            psql.array_agg(
                psql.aggregate_order_by(
                    F.json_build_object(
                        "product_id", board_product_info.c.product_id,
                        "advertiser_name", board_product_info.c.advertiser_name,
                        "is_active", board_product_info.c.is_active,
                        "product_image_url", board_product_info.c.product_image_url
                    ),
                    board_product_info.c.row_number.asc()
                )
            ).label('products'),
        ) \
        .group_by(
            board_product_info.c.tmp_id_col
        )
    return product_previews

def get_ordered_products_batch(
    pids_and_order_col_cte: CTE, 
    order_col_name: str,
    args: dict,
    desc: bool = True
    ) -> Select:

    products = qutils.join_product_info(pids_and_order_col_cte).cte()
    filtered_products = qutils.apply_filters(
        products,
        args,
        active_only=False
    ).cte()
    t = literal_column(order_col_name) 
    order_col = t.desc() if desc else t

    ## Order Products
    products_batch_ordered = s.select(filtered_products) \
        .order_by(
            order_col,
            filtered_products.c.product_id.desc()
        )
    
    return products_batch_ordered
