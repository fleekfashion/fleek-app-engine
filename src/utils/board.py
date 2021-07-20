import typing as t
import random

import sqlalchemy as s
from sqlalchemy.sql.selectable import Alias, CTE, Select
from sqlalchemy import subquery 
from sqlalchemy import Column
from sqlalchemy.orm.session import Session
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F
from sqlalchemy.sql.expression import literal, literal_column
from sqlalchemy.dialects.postgresql import array
from werkzeug.datastructures import ImmutableMultiDict

from src.utils import user_info
from src.utils import static 

def join_board_stats(board_info: CTE, board_products: CTE) -> Select:
    board_products = s.select(q.c.board_id, p.BoardProduct.product_id) \
        .filter(q.c.board_id == p.BoardProduct.board_id ) \
        .cte()
    q3 = qutils.join_base_product_info(board_products).cte()

    advertiser_stats = s.select(
        q3.c.board_id,
        q3.c.advertiser_name,
        F.count(q3.c.product_id).label('n_products')
    ).group_by(q3.c.board_id, q3.c.advertiser_name) \
        .cte()

    board_stats = s.select(
        advertiser_stats.c.board_id,
        F.sum(advertiser_stats.c.n_products).label('n_products'),
        postgresql.array_agg(
            F.json_build_object(
                'advertiser_name', advertiser_stats.c.advertiser_name,
                'n_products', advertiser_stats.c.n_products
            )
        ).label('advertiser_stats'),
    ) \
        .group_by(advertiser_stats.c.board_id) \
        .cte()

    return s.select(
        board_info,
        F.coalesce(board_stats.c.n_products, 0).label('n_products'),
        F.coalesce(board_stats.c.advertiser_stats, []).label('advertiser_stats')
    ).outerjoin(board_stats, board_info.c.board_id == board_stats.c.board_id)
