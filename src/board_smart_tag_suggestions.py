import typing as t
import itertools

import sqlalchemy as s
from sqlalchemy.sql.selectable import Alias, CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F 
from sqlalchemy.sql.expression import literal_column

from src.utils import string_parser, query as qutils
from src.utils import hashers
from src.defs import postgres as p

def _get_bad_smart_tags(board_id: str) -> Select:
    board_smart_tags = s.select(p.BoardSmartTag.smart_tag_id) \
        .where(p.BoardSmartTag.board_id == board_id)

    existing_tags = s.select(p.SmartTag.suggestion) \
        .where(p.SmartTag.smart_tag_id.in_(board_smart_tags)) \
        .cte()

    return s.select(
        p.SmartTag.smart_tag_id, 
    ).where(
        s.or_(
            p.SmartTag.suggestion.like('%'+existing_tags.c.suggestion+'%'),
            existing_tags.c.suggestion.like('%'+p.SmartTag.suggestion+'%')
        )
    )
def getBoardSmartTagSuggestions(args: dict) -> dict:
    board_id = args['board_id']
    limit = 6
    strong_cutoff = .6

    ## Helper miniqueries
    board_smart_tags = s.select(p.BoardSmartTag.smart_tag_id) \
        .where(p.BoardSmartTag.board_id == board_id)
    board_products = s.select(p.BoardProduct.product_id) \
            .where(p.BoardProduct.board_id == board_id)
    tagged_products = s.select(p.ProductSmartTag.product_id) \
        .where(p.ProductSmartTag.product_id.in_(board_products)) \
        .where(p.ProductSmartTag.smart_tag_id.in_(board_smart_tags))
    n_products = s.select(F.count(p.BoardProduct.product_id).label('n_products')) \
            .where(p.BoardProduct.board_id == board_id) \
            .group_by(p.BoardProduct.board_id) \
            .cte('board_product_count')

    ## Get the number of products per tag in the board
    ## Filter out products with tags that are already added
    board_tag_count = s.select(
        p.ProductSmartTag.smart_tag_id,
        F.count(p.ProductSmartTag.product_id).label('c')
    ) \
        .where(p.ProductSmartTag.product_id.in_(board_products)) \
        .where(~p.ProductSmartTag.product_id.in_(tagged_products)) \
        .where(~p.ProductSmartTag.smart_tag_id.in_(_get_bad_smart_tags(board_id))) \
        .group_by(p.ProductSmartTag.smart_tag_id) \
        .cte()


    ## Order by strong suggestions, then by normalized rank
    ranked = s.select(
        board_tag_count.c.smart_tag_id,
        p.SmartTag.suggestion,
        p.SmartTag.product_label,
        board_tag_count.c.c.label('n_products'),
        (board_tag_count.c.c/F.sqrt(p.SmartTag.n_hits)).label('score'),
        ((1.0*board_tag_count.c.c/n_products.c.n_products) > strong_cutoff).label('is_strong_suggestion')
        
    ).join(
            p.SmartTag,
            board_tag_count.c.smart_tag_id==p.SmartTag.smart_tag_id
    ).join(
            n_products,
            board_tag_count.c.c <= 10*n_products.c.n_products ## stupid condition to silence error
    ) \
        .where(board_tag_count.c.c > 1) \
        .order_by(
            literal_column('is_strong_suggestion').desc(), 
            literal_column('score').desc()
    ).limit(limit)

    res = run_query(ranked)
    res = string_parser._process_smart_tags(res)
    is_strong = len(res) > 0 and res[0]['is_strong_suggestion']
    return {
        'suggestions': res,
        'is_strong': is_strong
    }
