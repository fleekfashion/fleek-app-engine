import typing as t
import random
import itertools

import sqlalchemy as s
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
        tmp_id_col.label('tmp_id'),
        q3.c.advertiser_name,
        F.count(q3.c.product_id).label('n_products')
    ).group_by(tmp_id_col, q3.c.advertiser_name) \
        .cte()

    ## Get total group level stats
    board_stats = s.select(
        advertiser_stats.c.tmp_id.label(id_colname),
        F.sum(advertiser_stats.c.n_products).label('n_products'),
        psql.array_agg(
            F.json_build_object(
                'advertiser_name', advertiser_stats.c.advertiser_name,
                'n_products', advertiser_stats.c.n_products
            )
        ).label('advertiser_stats'),
    ) \
        .group_by(advertiser_stats.c.tmp_id)

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
    product_cols = [(c.name, c) for c in board_product_info.c if 'tmp_id_col' not in c.name ]
    product_cols_json_agg = list(itertools.chain(*product_cols))
    product_previews = s.select(
            board_product_info.c.tmp_id_col.label(id_col),
            psql.array_agg(
                psql.aggregate_order_by(
                    F.json_build_object(*product_cols_json_agg),
                    board_product_info.c.row_number.desc()
                )
            ).label('products'),
        ) \
        .group_by(
            board_product_info.c.tmp_id_col
        )
    return product_previews