import typing as t
import cachetools.func
from functional import seq
from src.utils.sqlalchemy_utils import session_scope
from sqlalchemy.sql import select, func, asc

from src.defs import postgres as p

@cachetools.func.ttl_cache(ttl=60*60)
def get_advertiser_counts() -> t.Dict[str, int]:
    with session_scope() as session:
        res = session.query(
            p.AdvertiserProductCount.advertiser_name,
            p.AdvertiserProductCount.n_products,
        ).all()

    advertiser_counts = seq(res) \
        .map(lambda x: (x['advertiser_name'], x['n_products'])) \
        .to_dict()
    return advertiser_counts

@cachetools.func.ttl_cache(ttl=60*60)
def get_advertiser_names() -> t.List[str]:
    with session_scope() as session:
        res = session.query(
            p.AdvertiserProductCount.advertiser_name,
        ).all()

    advertiser_counts = seq(res) \
        .map(lambda x: (x['advertiser_name'])) \
        .to_list()
    return advertiser_counts
