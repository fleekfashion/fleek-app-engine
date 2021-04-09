from src.utils.sqlalchemy_utils import load_session, row_to_dict
from src.utils import hashers
from src.defs import postgres as p
from datetime import datetime as dt
import itertools
from sqlalchemy.dialects import postgresql
from sqlalchemy import func

def _add_sizes_to_products(session, query):
    products_subquery = query.subquery()
    sizes_subquery = session.query(
        p.ProductSizeInfo.product_id,
        postgresql.array_agg(
            func.json_build_object(
                p.ProductSizeInfo.size,
                p.ProductSizeInfo.product_purchase_url,
                p.ProductSizeInfo.in_stock,
            )
        ).label('sizes')
    ).filter(
        p.ProductSizeInfo.product_id == products_subquery.c.product_id
    ) \
     .group_by(p.ProductSizeInfo.product_id) \
     .subquery()

    return session.query(products_subquery) \
                  .join(sizes_subquery, sizes_subquery.c.product_id == products_subquery.c.product_id, isouter=True)

def getBoardInfo(args: dict) -> dict:
    board_id = args['board_id']

    session = load_session()
    board = session.query(p.Board).filter(p.Board.board_id == board_id).first()
    print(board)

    return row_to_dict(board) if board else {'success': False}

def getBoardProductsBatch(args: dict) -> dict:
    board_id = args['board_id']
    offset = args['offset']
    limit = args['limit']

    session = load_session()
    products_query = session.query(p.ProductInfo) \
                      .join(p.BoardProduct) \
                      .filter(p.BoardProduct.product_id == p.ProductInfo.product_id) \
                      .filter(p.BoardProduct.board_id == board_id) \
                      .order_by(p.BoardProduct.last_modified_timestamp.desc())
    products_with_sizes_query = _add_sizes_to_products(session,products_query)
    products_batch = products_with_sizes_query.limit(limit).offset(offset).all()
    print(products_batch[0]._asdict())

    return [row_to_dict(row) for row in products_batch]