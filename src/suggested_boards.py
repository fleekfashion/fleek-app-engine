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

def _get_existing_user_smart_tags(user_id: int) -> Select:
    return s.select(p.BoardSmartTag.smart_tag_id) \
        .where(
            p.BoardSmartTag.board_id.in_(
                s.select(p.UserBoard.board_id) \
                    .where(p.UserBoard.user_id == user_id)
            )
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
        psql.array_agg(
            psql.aggregate_order_by(
                products.c.product_id,
                products.c.event_timestamp.desc(),
                products.c.product_id.desc()
            )
        )[0:1].label('pids'),
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
        p.SmartTag.product_label,
        smart_tags.c.n_products,
        smart_tags.c.pids,
    ).join(p.SmartTag, smart_tags.c.smart_tag_id==p.SmartTag.smart_tag_id) \
        .where(smart_tags.c.n_products >= MIN_PRODUCTS) \
        .cte()
    t2 = normalized_smart_tags.alias('t2')

    ## Remove duplicate ids (boards with matching previews)
    duplicate_ids = s.select(
        normalized_smart_tags.c.smart_tag_id,
    ) \
        .join(t2, normalized_smart_tags.c.pids == t2.c.pids) \
        .where(
            s.or_(
                normalized_smart_tags.c.score < t2.c.score, ## Take the item with lower score (to remove)
                s.and_(
                    normalized_smart_tags.c.suggestion > t2.c.suggestion, ## Tie breaker is suggestion (arbitrary)
                    normalized_smart_tags.c.score == t2.c.score
                )
            )
        ) \
        .where(~normalized_smart_tags.c.smart_tag_id.in_( ## Do not sugg
            _get_existing_user_smart_tags(user_id)
        )) \
        .distinct()

    ## Order the smarttags with random seeding
    ordered_smart_tags = s.select(
        normalized_smart_tags.c.smart_tag_id,
        normalized_smart_tags.c.score,
        normalized_smart_tags.c.pids,
        normalized_smart_tags.c.suggestion.label('name'),
        normalized_smart_tags.c.product_label,
        F.setseed(qutils.get_daily_random_seed())
    ) \
        .where(~normalized_smart_tags.c.smart_tag_id.in_(duplicate_ids)) \
        .order_by( (
            F.power(normalized_smart_tags.c.score, SCORE_POWER)
        ).desc()
    ) \
        .offset(offset) \
        .limit(limit)
    return ordered_smart_tags 


def _get_smart_tag_products(ranked_smart_tags, user_id: int) -> Select:
    ## Get relevent smart products
    smart_products = s.select(p.ProductSmartTag) \
        .where(
            p.ProductSmartTag.smart_tag_id.in_(
                s.select(ranked_smart_tags.c.smart_tag_id)
            )
        ).cte()

    ## Get smart products in user faves
    ## And get the top 6
    pids = s.select(
        smart_products,
        p.UserProductFaves.event_timestamp.label('last_modified_timestamp'),
    ).join(
        p.UserProductFaves, 
        p.UserProductFaves.product_id == smart_products.c.product_id,
    ) \
        .where(p.UserProductFaves.user_id == user_id) \
        .cte()
    return pids


def getSuggestedBoardsBatch(args: dict, dev_mode: bool=False) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id']) if not dev_mode else args['user_id']
    offset = args['offset']
    limit = args['limit']

    ranked_smart_tags = get_ranked_user_smart_tags(user_id, offset, limit, rand=True).cte()
    smart_tag_products = _get_smart_tag_products(ranked_smart_tags, user_id)

    product_previews = board.get_product_previews(
            smart_tag_products,
            "smart_tag_id",
            literal_column("last_modified_timestamp").desc()
        ).cte()
    tag_stats = board.get_product_group_stats(
            smart_tag_products, 
            "smart_tag_id").cte()

    q = s.select(
        ranked_smart_tags,
        tag_stats.c.n_products,
        tag_stats.c.advertiser_stats,
        product_previews.c.products,
        literal(True).label("isOwner")
    ) \
        .join(product_previews, tag_stats.c.smart_tag_id == product_previews.c.smart_tag_id) \
        .join(ranked_smart_tags, tag_stats.c.smart_tag_id == ranked_smart_tags.c.smart_tag_id) \
        .order_by(ranked_smart_tags.c.score.desc())

    boards = run_query(q)
    parsed_boards = string_parser.process_suggested_boards(boards) 
    return {
        'boards': parsed_boards 
    }

def getUserSmartTagProductBatch(args: dict, dev_mode: bool=False) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id']) if not dev_mode \
            else args['user_id']
    smart_tag_id = args['smart_tag_id']
    offset = args['offset']
    limit = args['limit']
    is_swipe_page = args.get('swipe_page', 'true').lower() == 'true'
    is_legacy = args.get('legacy', 'true').lower() == 'true'

    smart_products = s.select(
        p.ProductSmartTag.product_id
    ).where(p.ProductSmartTag.smart_tag_id == smart_tag_id)

    user_faves = s.select(
        p.UserProductFaves.product_id,
        p.UserProductFaves.event_timestamp.label('last_modified_timestamp')
    ) \
        .where(p.UserProductFaves.user_id == user_id) \
        .where(p.UserProductFaves.product_id.in_(smart_products)) \
        .cte()

    products = qutils.join_product_info(user_faves).cte()
    filtered_products = qutils.apply_filters(
        products,
        args,
        active_only=False
    ).cte()

    ## Order Products
    products_batch_ordered = s.select(filtered_products) \
        .order_by(
            filtered_products.c.last_modified_timestamp.desc(),
            filtered_products.c.product_id.desc()
        ) \
        .limit(limit) \
        .offset(offset)
    select_product_cols = qutils.select_product_fields(products_batch_ordered, is_swipe_page, is_legacy)

    result = run_query(select_product_cols)
    return {
        "products": result
    }
