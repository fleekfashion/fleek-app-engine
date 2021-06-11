import typing as t

import sqlalchemy as s
from src.utils import query as qutils 
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F
import itertools

def getBoardSuggestions(args: dict) -> dict:
    board_id = args['board_id']
    user_id = args['user_id']
    offset = args['offset']
    limit = args['limit']
    
    MAX_PRODUCTS = 50

    board_products = s.select(p.BoardProduct) \
        .filter(p.BoardProduct.board_id == board_id) \
        .order_by(p.BoardProduct.last_modified_timestamp.desc()) \
        .limit(MAX_PRODUCTS) \
        .cte()
    
    similar_products = s.select(
        p.SimilarItems.similar_product_id.label('product_id'),
        F.count(board_products.c.product_id).label('n_iter')
    ) \
        .join(board_products, board_products.c.product_id == p.SimilarItems.product_id) \
        .group_by(p.SimilarItems.similar_product_id) \
        .cte()

    ranked_products = s.select(
        similar_products.c.product_id
    ) \
        .order_by(similar_products.c.n_iter) \
        .cte()

    pinfo  = qutils.join_product_info(ranked_products).cte()

    products = s.select(pinfo) \
        .filter(pinfo.c.is_active == True) \
        .offset(offset) \
        .limit(limit)

    result = run_query(products)
    return {
        "products": result
    }

