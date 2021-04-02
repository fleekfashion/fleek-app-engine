import cachetools.func
from functional import seq
from sqlalchemy.sql import select, func, asc

from src.defs import postgres as p

@cachetools.func.ttl_cache(ttl=60*60)
def get_advertiser_counts():
    query = p.ADVERTISER_PRODUCT_COUNT_TABLE \
                .select()
    res = p.engine.connect().execute(query)
    advertiser_counts = seq(res) \
        .map(lambda x: (x['advertiser_name'], x['n_products'])) \
        .to_dict()
    return advertiser_counts

@cachetools.func.ttl_cache(ttl=60*60)
def get_advertiser_names():
    query = p.ADVERTISER_PRODUCT_COUNT_TABLE \
                .select() \
                .with_only_columns([p.ADVERTISER_PRODUCT_COUNT_TABLE.c.advertiser_name]) \
                .order_by(func.lower("advertiser_name"))

    res = p.engine.connect().execute(query)
    parsed_res = [ row["advertiser_name"] for row in res ]
    return parsed_res
