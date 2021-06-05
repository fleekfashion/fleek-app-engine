from src.defs import postgres as p
import sqlalchemy as s
import src.utils.query as qutils
from src.utils.sqlalchemy_utils import run_query

def getOrdersFromAdvertiser(args: dict) -> dict:
    advertiser_id = args['advertiser_id']
    offset = args['offset']
    limit = args['limit']

    order_product_cte = s.select(
        p.OrderProduct
    ) \
    .cte()

    order_product_info_cte = qutils.join_product_info(order_product_cte).cte()

    order_product_filtered_by_advertiser_cte = s.select(
        order_product_info_cte
    ) \
    .join(p.Advertiser, order_product_info_cte.c.advertiser_name == p.Advertiser.advertiser_name) \
    .filter(p.Advertiser.advertiser_id == advertiser_id) \
    .cte()

    order_final_query = s.select(
        order_product_filtered_by_advertiser_cte,
        *p.Order.__table__.columns
    ) \
    .join(p.Order, p.Order.order_id == order_product_filtered_by_advertiser_cte.c.order_id) \
    .offset(offset) \
    .limit(limit)

    res = run_query(order_final_query)

    return res