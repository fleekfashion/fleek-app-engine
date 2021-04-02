import typing as t

import cachetools.func
from functional import seq
from sqlalchemy.sql import select, func, asc

from src.defs import postgres as p

@cachetools.func.ttl_cache(ttl=60*60)
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
