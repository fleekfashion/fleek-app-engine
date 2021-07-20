import typing as t

import sqlalchemy as s
from src.utils import query as qutils 
from src.utils.board import get_product_group_stats, get_product_previews
from sqlalchemy.sql.selectable import Alias, CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy import func as F 
import itertools

def _get_boards_info(boards: CTE) -> Select:
    board_ids = s.select(boards.c.board_id)
    board_products = s.select(p.BoardProduct.board_id, p.BoardProduct.product_id) \
        .filter(p.BoardProduct.board_id.in_(
            s.select(boards.c.board_id)
            )
        ) \
        .cte()
    board_stats = get_product_group_stats(board_products, 'board_id').cte()
    board_info = s.select(
        p.Board.__table__,
        F.coalesce(board_stats.c.n_products, 0).label('n_products'),
        F.coalesce(board_stats.c.advertiser_stats, []).label('advertiser_stats')
    ) \
        .where(p.Board.board_id.in_(board_ids)) \
        .outerjoin(board_stats, board_stats.c.board_id == p.Board.board_id)
    return board_info


def getBoardInfo(args: dict) -> dict:
    board_id = args['board_id']
    basic_board = s.select(p.Board.board_id).filter(p.Board.board_id == board_id).cte()
    board = _get_boards_info(basic_board)
    result = get_first(board)
    parsed_res = result if result else {"error": "invalid collection id"}
    return parsed_res

def getBoardProductsBatch(args: dict) -> dict:
    board_id = args['board_id']
    offset = args['offset']
    limit = args['limit']

    board_pids_query = s.select(
        p.BoardProduct.product_id, 
        p.BoardProduct.last_modified_timestamp
    ) \
        .filter(p.BoardProduct.board_id == board_id) \
        .cte()

    ## Get and filter products
    products = qutils.join_product_info(board_pids_query).cte()
    filtered_products = qutils.apply_filters(
        products,
        args,
        active_only=False
    ).cte()

    ## Order Products
    products_batch_ordered = s.select(filtered_products) \
        .order_by(
            filtered_products.c.last_modified_timestamp.desc(),
            filtered_products.c.product_id.desc()
        ) \
        .limit(limit) \
        .offset(offset)
    result = run_query(products_batch_ordered)
    return {
        "products": result
    }


def getUserBoardsBatch(args: dict, dev_mode: bool = False) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id']) if not dev_mode else args['user_id']
    offset = args['offset']
    limit = args['limit']

    user_board_ids = s.select(p.UserBoard.board_id) \
        .filter(p.UserBoard.user_id == user_id) \

    ## Get current boards batch
    boards_batch = s.select(
            p.Board.board_id,
        ) \
        .where(p.Board.board_id.in_(user_board_ids)) \
        .order_by(p.Board.last_modified_timestamp.desc()) \
        .limit(limit) \
        .offset(offset) \

    ## Get products from relevant boards
    board_products = s.select(
            p.BoardProduct.board_id,
            p.BoardProduct.product_id,
            p.BoardProduct.last_modified_timestamp,
        ) \
        .filter(
            p.BoardProduct.board_id.in_(boards_batch)
        ) \
        .cte()
    product_previews = get_product_previews(
            board_products,
            'board_id',
            'last_modified_timestamp',
            desc=True
    ).cte()
    
    print(product_previews)
    ## Join board info with the board products
    board_info = _get_boards_info(product_previews).cte()
    boards = s.select(
            board_info,
            product_previews.c.products
        ) \
        .outerjoin(product_previews, product_previews.c.board_id == board_info.c.board_id) \
        .cte()

    boards_ordered = s.select(boards) \
            .order_by(boards.c.last_modified_timestamp.desc())
    result = run_query(boards_ordered)
    
    return {
        "boards": result
    }
