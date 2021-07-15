import typing as t

import sqlalchemy as s
from src.utils import query as qutils 
from sqlalchemy.sql.selectable import Alias, CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F 
import itertools

def _get_user_board_products(user_id: int) -> Select:
    ## Get products already in boards
    return s.select(
        p.BoardProduct.product_id
    ).where(p.BoardProduct.board_id.in_(
        s.select(p.UserBoard.board_id) \
            .where(p.UserBoard.user_id == user_id)
    ))

def _get_filtered_products(user_id: int) -> Select:
    ## Get relevent product
    return s.select(
        p.UserProductFaves
    )\
        .where(p.UserProductFaves.user_id == user_id) \
        .where(
            p.UserProductFaves.event_timestamp > 
            qutils.days_ago_timestamp(60)
        ) \
        .where(
            ~p.UserProductFaves.product_id.in_(_get_user_board_products(user_id))
        )


def ranked_user_smart_tags(user_id: int, offset: int, limit: int, rand: bool = True) -> Select:
    NORMALIZATION = .5
    MIN_PRODUCTS = 3
    SCORE_POWER = 1

    products = _get_filtered_products(user_id).cte('products')

    ## Get timeweighted smart tag scores (via n_products)
    smart_tags = s.select(
        F.sum(
            1.0 / (
                1 + (qutils.days_ago_timestamp(0) - products.c.event_timestamp)/(60*60*24)
            )
        ).label('score'),
        F.count(products.c.product_id).label('n_products'),
        p.ProductSmartTag.smart_tag_id
    ).join(
        p.ProductSmartTag,
        products.c.product_id == p.ProductSmartTag.product_id
    ).group_by(p.ProductSmartTag.smart_tag_id) \
        .cte()

    ## Get normalized smart tag scores
    normalized_smart_tags = s.select(
        smart_tags.c.smart_tag_id,
        (smart_tags.c.score/F.power(p.SmartTag.n_hits, NORMALIZATION)).label('score'),
        p.SmartTag.suggestion,
        smart_tags.c.n_products
    ).join(p.SmartTag, smart_tags.c.smart_tag_id==p.SmartTag.smart_tag_id) \
        .where(smart_tags.c.n_products >= MIN_PRODUCTS) \
        .cte()

    ## Order the smarttags with random seeding
    ordered_smart_tags = s.select(
        normalized_smart_tags.c.smart_tag_id,
        normalized_smart_tags.c.score,
        normalized_smart_tags.c.suggestion,
        F.setseed(qutils.get_daily_random_seed())
    ) \
        .order_by( (
            F.random()*F.power(normalized_smart_tags.c.score, SCORE_POWER) if rand \
                else F.power(normalized_smart_tags.c.score, SCORE_POWER)
        ).desc()
    ) \
        .offset(offset) \
        .limit(limit)
    return ordered_smart_tags
