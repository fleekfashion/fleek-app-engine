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
    products = s.select(p.UserProductFaves) \
        .where(p.UserProductFaves.user_id == user_id) \
        .order_by(p.UserProductFaves.event_timestamp.desc()) \
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

def getExploreBoardsBatch(args):
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])

    smart_tags = qutils.union_by_names(
        get_fave_smart_tags(user_id),
        get_random_smart_tags()
    ).order_by(F.random()*literal_column('c').asc()) \
        .limit(30) \
        .cte()

