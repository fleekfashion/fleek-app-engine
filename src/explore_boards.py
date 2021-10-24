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

def get_fave_smart_tags(user_id) -> Select:
    products = s.select(
        p.UserProductFaves,
        F.setseed(qutils.get_daily_random_seed()),
        ) \
        .where(p.UserProductFaves.user_id == user_id) \
        .order_by(
            F.random() *
            F.power(
                (
                    qutils.days_ago_timestamp(0) -
                    p.UserProductFaves.event_timestamp
                ) / (24*60*60)
                + 1,
                2
            ).asc()
        ) \
        .limit(100) \
        .cte()

    smart_tags = s.select(
        p.ProductSmartTag.smart_tag_id,
        F.count(p.ProductSmartTag.smart_tag_id).label('c')
    ).where(p.ProductSmartTag.product_id == products.c.product_id) \
        .group_by(p.ProductSmartTag.smart_tag_id)
    return smart_tags

def get_random_smart_tags() -> Select:
    q = s.select(
        p.SmartTag.smart_tag_id,
        literal(1).label('c')
    ) \
        .order_by(F.random()) \
        .limit(100)
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
        get_fave_smart_tags(user_id),
        get_random_smart_tags()
    ).cte()

    processed_tags = s.select(
        smart_tags,
        p.SmartTag.suggestion.label('name'),
        p.SmartTag.product_label.label('product_label'),
        F.setseed(qutils.get_daily_random_seed()),
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
