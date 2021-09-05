import typing as t
import time
from src.defs import postgres as p

import sqlalchemy as s
from sqlalchemy.sql.expression import literal
from sqlalchemy.sql import text
import src.utils.query as qutils
from src.utils.sqlalchemy_utils import run_query 

def getSimilarProducts(args: dict) -> t.List[dict]:
    product_id = args['product_id']
    offset = args.get('offset', 0)
    limit = args.get('limit', 50)
    is_swipe_page = args.get('swipe_page', 'true').lower() == 'true'
    is_legacy = args.get('legacy', 'true').lower() == 'true'

    sim_pids = s.select(
        p.SimilarItems.similar_product_id.label('product_id')
    ) \
        .filter(p.SimilarItems.product_id == literal(product_id) ) \
        .offset(offset) \
        .limit(limit) \
        .cte('similar_product_ids')
        
    similar_products_query = qutils.join_product_info(sim_pids).cte()
    filtered_products = qutils.apply_filters(similar_products_query, args, active_only=True).cte()
    select_product_cols = qutils.select_product_fields(filtered_products, is_swipe_page, is_legacy)

    return run_query(select_product_cols)
