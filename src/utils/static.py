import typing as t
import cachetools.func
from functional import seq
from src.utils.sqlalchemy_utils import session_scope, run_query
from sqlalchemy.sql import select, func, asc
import sqlalchemy as s
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

@cachetools.func.ttl_cache(ttl=12*60*60)
def _get_advertiser_price_quantiles(quantile: float) -> t.Dict[str, float]:
    q = s.select(
        func.percentile_cont(quantile) \
            .within_group(p.ProductInfo.product_sale_price.asc()) \
            .label('value'),
        p.ProductInfo.advertiser_name
    ).group_by(p.ProductInfo.advertiser_name)
    res = run_query(q)
    return seq(res).map(lambda x:
        (x['advertiser_name'], x['value'])
    ).to_dict()

def get_advertiser_price_quantile(advertiser_name: str, quantile: float) -> float:
    return _get_advertiser_price_quantiles(quantile) \
            .get(advertiser_name, 0)
