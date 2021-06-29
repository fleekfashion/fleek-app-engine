import typing as t
import itertools

import sqlalchemy as s
from sqlalchemy.sql.expression import literal, literal_column
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F
from werkzeug.datastructures import ImmutableMultiDict

from src.utils import query as qutils 
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from src.defs.search import index 
from src.productSearch import productSearch

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

def getBoardSuggestionsSecondaryLabels(args: dict) -> dict:
    board_id = args['board_id']
    user_id = args['user_id']
    offset = args['offset']
    limit = args['limit']
    MAX_PRODUCTS = 50
    
    board_products = s.select(p.BoardProduct) \
        .filter(p.BoardProduct.board_id == board_id) \
        .limit(MAX_PRODUCTS) \
        .cte()

    similar_products = s.select(
        p.SimilarItems.similar_product_id.label('product_id'),
        ) \
        .join(board_products, board_products.c.product_id == p.SimilarItems.product_id) \
        .group_by(p.SimilarItems.similar_product_id) \
        .cte()

    q = s.select(
        p.ProductSecondaryLabels.product_secondary_label,
        F.count(p.ProductSecondaryLabels.product_id).label('n_products')
    ) \
        .where(p.BoardProduct.board_id == board_id) \
        .where(p.BoardProduct.product_id == p.ProductSecondaryLabels.product_id) \
        .group_by(p.ProductSecondaryLabels.product_secondary_label) \
        .order_by(literal_column('n_products').desc()) \
        .limit(20) \
        .cte()

    ranked_products = s.select(
        F.sum(q.c.n_products).label('score'),
        p.ProductSecondaryLabels.product_id
    ) \
        .where(
            p.ProductSecondaryLabels.product_id.in_(
                s.select(similar_products.c.product_id)
            )
        ) \
        .where(p.ProductSecondaryLabels.product_secondary_label == q.c.product_secondary_label) \
        .where(
            ~p.ProductSecondaryLabels.product_id.in_(
                s.select(p.BoardProduct.product_id) \
                    .filter(p.BoardProduct.board_id == board_id)
            )
        ) \
        .group_by(p.ProductSecondaryLabels.product_id) \
        .cte()

    ranked_products = s.select(
        ranked_products
    ).order_by(ranked_products.c.score.desc()) \
        .cte()

    pinfo  = qutils.join_product_info(ranked_products).cte()
    products = s.select(pinfo) \
        .filter(pinfo.c.is_active == True) \
        .offset(offset) \
        .limit(limit)

    result = run_query(products)

    res2 = result if len(result) > 0 else productSearch(
        index=index, 
        args=ImmutableMultiDict({
            'searchString': '',
            'offset': offset,
            'limit': limit
        })
    )
    return {
        "products": res2 
    }
