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


def get_ranked_user_smart_tags(user_id: int, offset: int, limit: int, rand: bool = True) -> Select:
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


def _get_smart_tag_products(ranked_smart_tags, user_id: int) -> Select:
    ## Get relevent smart products
    smart_products = s.select(p.ProductSmartTag) \
        .where(p.ProductSmartTag.smart_tag_id.in_(
                s.select(ranked_smart_tags.c.smart_tag_id)
            )
        ).cte()

    ## Get smart products in user faves
    ## And get the top 6
    pids = s.select(
        smart_products,
        p.UserProductFaves.event_timestamp.label('last_modified_timestamp'),
         F.row_number().over(
             smart_products.c.smart_tag_id, 
             order_by=(
                 p.UserProductFaves.event_timestamp.desc(), 
                 p.UserProductFaves.product_id.desc()
             )
         ).label('row_number')
    ).join(
        p.UserProductFaves, 
        p.UserProductFaves.product_id == smart_products.c.product_id,
    ) \
        .where(p.UserProductFaves.user_id == user_id) \
        .cte()

    ## Get top 6 for each smart tag
    filtered_pids = s.select(pids) \
        .where(pids.c.row_number <= 6) \
        .order_by(pids.c.smart_tag_id, pids.c.row_number) \
        .cte()
    products  = qutils.join_product_info(filtered_pids)
    return products

def join_product_preview(ranked_smart_tags: CTE, user_id: int) -> Select:
    products = _get_smart_tag_products(ranked_smart_tags, user_id).cte()

    product_cols = [(c.name, c) for c in products.c if c.name not in ranked_smart_tags.c ]
    product_cols_json_agg = list(itertools.chain(*product_cols))
    product_preview = s.select(
        products.c.smart_tag_id,
        postgresql.array_agg(
            F.json_build_object(*product_cols_json_agg)
        ).label('products')
    ).group_by(products.c.smart_tag_id) \
        .cte()

    q = s.select(
        ranked_smart_tags,
        product_preview.c.products
    ).join(product_preview, 
        ranked_smart_tags.c.smart_tag_id==product_preview.c.smart_tag_id
    ).order_by(ranked_smart_tags.c.score.desc())
    return q

def getSuggestedBoardsBatch(args: dict, dev_mode: bool=False) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id']) if not dev_mode else args['user_id']
    offset = args['offset']
    limit = args['limit']

    ranked_smart_tags = get_ranked_user_smart_tags(user_id, offset, limit, rand=True).cte()
    q = join_product_preview(ranked_smart_tags, user_id)
    result = run_query(q)
    boards = [ {**b, 'products': qutils.sort_product_preview(b['products']) } for b in result ]
    return {
        'boards': boards
    }
