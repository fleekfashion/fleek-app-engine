import typing as t
import itertools

import sqlalchemy as s
from sqlalchemy.sql.selectable import Alias, CTE, Select
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy import func as F 
from sqlalchemy.sql.expression import literal, literal_column

from src.utils import string_parser, board, query as qutils 
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from src.defs.types.board_type import BoardType

def get_fave_smart_tags(user_id: int, limit: int, power: float=2) -> Select:
    t = s.select(
        p.UserProductFaves,
        F.setseed(qutils.get_daily_random_seed()),
    ).cte()

    products = s.select(
        t,
        F.random().label('res'),
        ) \
        .where(t.c.user_id == user_id) \
        .order_by(
            F.random() *
            F.power(
                (
                    qutils.days_ago_timestamp(0) -
                    t.c.event_timestamp
                ) / (24*60*60)
                + 1,
                power
            ).asc()
        ) \
        .limit(limit) \
        .cte()

    smart_tags = s.select(
        p.ProductSmartTag.smart_tag_id,
        F.count(p.ProductSmartTag.smart_tag_id).label('c'),
    ).where(p.ProductSmartTag.product_id == products.c.product_id) \
        .group_by(p.ProductSmartTag.smart_tag_id)
    return smart_tags

def get_random_smart_tags() -> Select:
    q = s.select(
        p.SmartTag.smart_tag_id,
        literal(1).label('c')
    ) \
        .order_by(F.random()) \
        .limit(50)
    return q

def _get_product_smart_tag(smart_tags: CTE) -> Select:
    relevent_products = s.select(
        p.ProductInfo.product_id,
        p.ProductInfo.advertiser_name,
        p.ProductSmartTag.smart_tag_id,
        p.ProductInfo.execution_date,
    ) \
        .where(p.ProductInfo.is_active) \
        .where(
            p.ProductSmartTag.smart_tag_id.in_(
                s.select(smart_tags.c.smart_tag_id)
            )
        ) \
        .join(
            p.ProductSmartTag,
            p.ProductInfo.product_id == p.ProductSmartTag.product_id
        )

    return relevent_products

def getExploreBoardsBatch(args):
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    offset = args['offset']
    limit = args['limit']

    smart_tags = qutils.union_by_names(
        get_fave_smart_tags(user_id, 100),
        get_random_smart_tags()
    ).cte()
    smart_tags = s.select(
        smart_tags,
        F.setseed(qutils.get_daily_random_seed()),
    ).cte()

    processed_tags = s.select(
        smart_tags,
        p.SmartTag.suggestion.label('name'),
        p.SmartTag.product_label.label('product_label'),
        F.row_number() \
            .over(
                order_by=(
                    F.power(smart_tags.c.c, 1)*F.random()
                ).desc()
            ).label("rank"),
    ) \
        .where(F.char_length(p.SmartTag.product_label) > 0) \
        .join(
            p.SmartTag,
            p.SmartTag.smart_tag_id == smart_tags.c.smart_tag_id
        ) \
        .cte()

    ranked_tags = s.select(
        processed_tags
    ) \
        .order_by(processed_tags.c.rank.asc()) \
        .offset(offset) \
        .limit(limit) \
        .cte()

    stat_products = _get_product_smart_tag(ranked_tags)

    preview_products = stat_products.where(
        literal_column('advertiser_name').in_(
            s.select(
                p.UserFavedBrands.advertiser_name
            ).where(p.UserFavedBrands.user_id == user_id)
        )
    )

    res = board.build_board_objects(
        ranked_tags,
        preview_products.cte(),
        stat_products.cte(),
        ['smart_tag_id'],
        literal_column('execution_date'),
        BoardType.SMART_TAG,
        True
    )
    boards = run_query(res)
    parsed_boards = string_parser.process_suggested_boards(boards)

    return {
        "smart_tag_boards": parsed_boards
    }


def _get_daily_mix_products(user_id):
    smart_tags = get_fave_smart_tags(user_id, 100, 1).cte()
    top_smart_tags = s.select(
        smart_tags,
    ).order_by( 
        (smart_tags.c.c).desc()
    ).limit(50) \
        .cte()

    relevent_products = _get_product_smart_tag(top_smart_tags) \
        .distinct(literal_column('product_id')) \
        .order_by(literal_column('product_id')) \
        .cte()
    products = qutils.join_product_info(relevent_products).cte()

    res = qutils.apply_ranking(
        products,
        user_id,
        pct=1.6,
        random_seed=qutils.get_daily_random_seed()
    ).limit(100)
    return res

def getDailyMixProductsBatch(args):
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    offset = args['offset']
    limit = args['limit']
    products = _get_daily_mix_products(user_id).cte()

    batch = s.select(products) \
        .offset(offset) \
        .limit(limit)

    res = run_query(batch)
    return {
        'products': res
    }


def getDailyMixPreview(args):
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    offset = args['offset']
    limit = args['limit']

    products = _get_daily_mix_products(user_id).cte()

    batch = s.select(products) \
        .limit(6)
    product_batch = run_query(batch)
    
    stats_q = board.get_product_group_stats(products, None,)
    stats = get_first(stats_q)

    return {
        **stats,
        'products': product_batch,
    }
