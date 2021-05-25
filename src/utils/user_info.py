import typing as t

from cachetools import LRUCache, cached

import cachetools.func
from functional import seq
from sqlalchemy.sql import select, func, asc
from sqlalchemy.sql.expression import literal

from src.defs import postgres as p
from src.utils.sqlalchemy_utils import session_scope, row_to_dict

USER_FAVED_BRANDS_CACHE: LRUCache = LRUCache(maxsize=2**6)

@cached(USER_FAVED_BRANDS_CACHE)
def get_user_fave_brands(user_id) -> t.List[str]:
    with session_scope() as session:
        res = session.query(p.UserFavedBrands.advertiser_name) \
            .filter(p.UserFavedBrands.user_id == literal(user_id)) \
            .all()
    return [ r['advertiser_name'] for r in res ]




def pop_user_fave_brands(user_id):
    if (user_id,) in USER_FAVED_BRANDS_CACHE.keys():
        USER_FAVED_BRANDS_CACHE.pop((user_id,))
