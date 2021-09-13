import typing as t
import itertools

import sqlalchemy as s
from src.utils import string_parser, board, query as qutils 
from sqlalchemy.sql.selectable import Alias, CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy import func as F 
from sqlalchemy.sql.expression import literal, literal_column

def _get_ranked_smart_tags(advertiser_name, offset, limit) -> Select:
    q = s.select(
        p.AdvertiserTopSmartTag.smart_tag_id,
        p.AdvertiserTopSmartTag.advertiser_name,
        p.AdvertiserTopSmartTag.score,
        F.setseed(qutils.get_daily_random_seed()),
        F.row_number() \
            .over(
                order_by=(
                    F.power(p.AdvertiserTopSmartTag.score, 2)*F.random()
                )
            ).label("rank"),
        p.SmartTag.suggestion.label('name'),
        p.SmartTag.product_label
    ) \
        .where(p.AdvertiserTopSmartTag.advertiser_name == advertiser_name) \
        .join(
            p.SmartTag,
            p.SmartTag.smart_tag_id == p.AdvertiserTopSmartTag.smart_tag_id
        ) \
        .cte()

    return s.select(q) \
        .order_by(q.c.rank.desc()) \
        .offset(offset) \
        .limit(limit)

def _get_product_smart_tag(ranked_tags: CTE) -> Select:
    relevent_products = s.select(
        p.ProductInfo.product_id,
        p.ProductInfo.advertiser_name,
        p.ProductSmartTag.smart_tag_id,
        p.ProductInfo.execution_date,
    ) \
        .where(p.ProductInfo.is_active) \
        .where(
            p.ProductSmartTag.smart_tag_id.in_(
                s.select(ranked_tags.c.smart_tag_id)
            )
        ) \
        .join(p.ProductSmartTag, p.ProductInfo.product_id == p.ProductSmartTag.product_id) \
        .join(
            ranked_tags,
            ranked_tags.c.advertiser_name == p.ProductInfo.advertiser_name
        )
    return relevent_products

def getAdvertiserTopBoardsBatch(args: dict):
    user_id = hashers.apple_id_to_user_id_hash(args["user_id"])
    advertiser_name = args["advertiser_name"]
    offset = args['offset']
    limit = args['limit']

    ranked_tags = _get_ranked_smart_tags(advertiser_name, offset, limit) \
        .cte()

    product_adv_tag = _get_product_smart_tag(ranked_tags).cte()

    product_previews = board.get_product_previews(
        product_adv_tag,
        ["smart_tag_id", "advertiser_name"],
        "execution_date"
    ).cte()

    tag_stats = board.get_product_group_stats(
            product_adv_tag, 
            "smart_tag_id"
    ).cte()

    q = s.select(
        ranked_tags,
        tag_stats.c.n_products,
        tag_stats.c.advertiser_stats,
        product_previews.c.products,
    ) \
        .join(product_previews, tag_stats.c.smart_tag_id == product_previews.c.smart_tag_id) \
        .join(ranked_tags, tag_stats.c.smart_tag_id == ranked_tags.c.smart_tag_id) \
        .order_by(ranked_tags.c.rank.desc())

    boards = run_query(q)
    parsed_boards = string_parser.process_suggested_boards(boards)

    return {
        "boards": parsed_boards
    }

