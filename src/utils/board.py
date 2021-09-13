import typing as t
import random
import itertools

import sqlalchemy as s
from sqlalchemy.sql.dml import Insert, Update
from sqlalchemy.sql.selectable import Alias, CTE, Select, ColumnClause
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

TMP_ID_COL = "tmp_id_colname"

def get_update_board_timestamp_stmt_from_select(board_ids: Select, timestamp: int) -> Update:
    return s.update(p.Board) \
        .where(p.Board.board_id.in_(board_ids)) \
        .values(last_modified_timestamp=timestamp) \
        .execution_options(synchronize_session=False)

def get_board_update_timestamp_statement(board_id: int, last_modified_timestamp: int) -> Update:
    return s.update(p.Board) \
        .where(p.Board.board_id == board_id) \
        .values(last_modified_timestamp=last_modified_timestamp)

def _map_colnames(
        q: CTE,
        c1: t.List[ColumnClause],
        c2: t.List[ColumnClause]
    ) -> t.List[ColumnClause]:
    return [ 
        q.c[real.name].label(tmp.name)
        if real.name != TMP_ID_COL
        else  literal(1).label(tmp.name)
        for real, tmp in zip(c1, c2)
    ]

def get_product_group_stats(
        products: CTE,
        id_col: t.Optional[str]
    ) -> Select:
    """
    Params
    -----------------------
    products: cte containing a list of product_ids
    id_col OPTIONAL: column name to do group bys

    Return
    -----------------
    Select with statistics on id_col level


    Compute a set of set of statistics such as
    advertiser distribution, total savings, n_products,
    TODO: label distribution
    """

    ## Process id col
    TMP_ID_COL = "tmp_id_colname"
    id_col = id_col if id_col else TMP_ID_COL
    id_col2 = [id_col] if isinstance(id_col, str) else id_col

    id_cols = [literal_column(c) for c in id_col2 ]
    tmp_ids = [ literal_column(f"tmp_{i}_{c}") for i, c in enumerate(id_col2) ]

    ## join pinfo
    q3 = qutils.join_base_product_info(products).cte()
    q3 = s.select(
        q3,
        *_map_colnames(q3, id_cols, tmp_ids)
    )

    ## Get advertiser level stats
    advertiser_stats = s.select(
        *tmp_ids,
        q3.c.advertiser_name,
        F.count(q3.c.product_id).label('n_products')
    ).group_by(*tmp_ids, q3.c.advertiser_name) \
        .cte()

    ## Get total group level stats
    full_advertiser_stats = s.select(
        *tmp_ids,
        F.sum(advertiser_stats.c.n_products).label('n_products'),
        psql.array_agg(
            F.json_build_object(
                'advertiser_name', advertiser_stats.c.advertiser_name,
                'n_products', advertiser_stats.c.n_products
            )
        ).label('advertiser_stats'),
    ) \
        .group_by(*tmp_ids) \
        .cte()

    active_stats = s.select(
        *tmp_ids,
        F.sum(q3.c.product_price - q3.c.product_sale_price).label('total_savings')
    ) \
        .where(q3.c.is_active) \
        .group_by(*tmp_ids) \
        .cte()

    board_stats = s.select(
        full_advertiser_stats.c.advertiser_stats,
        full_advertiser_stats.c.n_products,
        active_stats.c.total_savings,
        *_map_colnames(full_advertiser_stats, tmp_ids, id_cols)
    ).outerjoin(
        active_stats,
        s.and_(
            *[ 
                full_advertiser_stats.c[c] == active_stats.c[c] 
                for c in tmp_ids
            ]
        )
    )

    return board_stats

def get_product_previews(
    products: CTE,
    id_col: t.Union[str, t.List[str]],
    order_field: str,
    desc: bool = True
    ) -> Select:
    """
    Params
    -----------------------
    products: cte containing a list of product_ids
    id_col OPTIONAL: column name  or list of column names
    to do group bys
    order_field: column name to order list by
    desc: whether to order in desc or asc

    Return sa set of product preview json objects
    grouped by the id_col field
    """

    ## Process id col
    id_col = id_col if id_col else TMP_ID_COL
    id_col2 = [id_col] if isinstance(id_col, str) else id_col
    print(id_col2)

    id_cols = [literal_column(c) for c in id_col2 ]
    tmp_ids = [ literal_column(f"tmp_{i}_{c}") for i, c in enumerate(id_col2) ]

    ## Order process
    t = literal_column(order_field)
    order_by_field = t.desc() if desc else t

    ## Convert to tmp ids
    products = s.select(
        products,
        *_map_colnames(products, id_cols, tmp_ids)
    ).cte()

    ## Get row numbers
    board_products = s.select(
            *tmp_ids,
            products.c.product_id,
            F.row_number() \
                .over(
                    partition_by=tmp_ids,
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
            *tmp_ids,
            psql.array_agg(
                psql.aggregate_order_by(
                    F.json_build_object(
                        "product_id", board_product_info.c.product_id,
                        "advertiser_name", board_product_info.c.advertiser_name,
                        "is_active", board_product_info.c.is_active,
                        "product_image_url", board_product_info.c.product_image_url,
                        "product_price", board_product_info.c.product_price,
                        "product_sale_price", board_product_info.c.product_sale_price,
                    ),
                    board_product_info.c.row_number.asc()
                )
            ).label('products'),
        ) \
        .group_by(
            *tmp_ids
        ) \
        .cte()

    return s.select(
        product_previews.c.products,
        *_map_colnames(product_previews, tmp_ids, id_cols)
    )

def get_ordered_products_batch(
    pids_and_order_col_cte: CTE,
    order_col_name: str,
    args: dict,
    desc: bool = True
    ) -> Select:
    """
    Params
    -----------------------
    products: cte containing a list of product_ids
    order_col_name: column name to order list by
    args: dict of filter args
    desc: whether to order in desc or asc

    Returns a select for a batch of products
    """
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
