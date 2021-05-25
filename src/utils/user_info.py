import typing as t

from cachetools import LRUCache, cached

import cachetools.func
from functional import seq
from sqlalchemy.sql import select, func, asc

from src.defs import postgres as p

USER_FAVED_BRANDS_CACHE: LRUCache = LRUCache(maxsize=2**6)

@cached(USER_FAVED_BRANDS_CACHE)
def get_user_fave_brands(user_id) -> t.List[str]:
    query = p.USER_FAVED_BRANDS_TABLE \
                .select() \
                .with_only_columns([p.USER_FAVED_BRANDS_TABLE.c.advertiser_name]) \
                .where(p.USER_FAVED_BRANDS_TABLE.c.user_id == user_id)
    res = p.engine.connect().execute(query)
    res2 = seq(res) \
        .map(lambda x: x['advertiser_name']) \
        .to_list()
    return res2

def pop_user_fave_brands(user_id):
    if (user_id,) in USER_FAVED_BRANDS_CACHE.keys():
        USER_FAVED_BRANDS_CACHE.pop((user_id,))

