from src.utils.sqlalchemy_utils import load_session, row_to_dict
from src.utils import hashers
from src.defs import postgres as p
from datetime import datetime as dt
import itertools


def getBoardInfo(args: dict) -> dict:
    board_id = args['board_id']

    session = load_session()
    board = session.query(p.Board).filter(p.Board.board_id == board_id).first()

    return row_to_dict(board) if board else {'success': False}

# getBoardProductsBatch(board_id, offset, limit)
# Dict
# List
# Dict
# PRODUCTS: same as other API

def getBoardProductsBatch(args: dict) -> dict:
    board_id = args['board_id']

    session = load_session()
    products = session.query(p.BoardProduct, p.ProductInfo) \
                      .filter(p.BoardProduct.product_id == p.ProductInfo.product_id) \
                      .filter(p.BoardProduct.board_id == board_id) \
                      .order_by(p.BoardProduct.last_modified_timestamp.desc()) \
                      .all()
    print(products)

    return [row_to_dict(row) for row in products]