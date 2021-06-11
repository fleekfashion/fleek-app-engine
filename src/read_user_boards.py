import typing as t

import sqlalchemy as s
from src.utils.query import join_product_info
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func 
import itertools

def getBoardInfo(args: dict) -> dict:
    board_id = args['board_id']
    board = s.select(p.Board).filter(p.Board.board_id == board_id).cte()
    result = get_first(s.select(board))
    parsed_res = result if result else {"error": "invalid collection id"}
    return parsed_res 

def getBoardProductsBatch(args: dict) -> dict:
    board_id = args['board_id']
    offset = args['offset']
    limit = args['limit']

    board_pids_query = s.select(p.BoardProduct.product_id) \
                    .filter(p.BoardProduct.board_id == board_id) \
                    .order_by(p.BoardProduct.last_modified_timestamp.desc()) \
                    .cte()
    products_batch = join_product_info(board_pids_query) \
        .limit(limit) \
        .offset(offset) 
    result = run_query(products_batch)
    return {
        "products": result
    }

def getUserBoardsBatch(args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    offset = args['offset']
    limit = args['limit']

    user_board_ids_subq = s.select(p.UserBoard.board_id, p.UserBoard.user_id) \
        .filter(p.UserBoard.user_id == user_id) \
        .order_by(p.UserBoard.last_modified_timestamp.desc()) \
        .offset(offset) \
        .limit(limit) \
        .cte()

    board_product_lateral_subq = s.select(p.BoardProduct.board_id, p.BoardProduct.product_id, p.BoardProduct.last_modified_timestamp) \
        .filter(p.BoardProduct.board_id == user_board_ids_subq.c.board_id) \
        .order_by(p.BoardProduct.last_modified_timestamp.desc()) \
        .limit(6) \
        .subquery() \
        .lateral()

    join_board_product_subq = s.select(board_product_lateral_subq, user_board_ids_subq.c.user_id) \
        .join(board_product_lateral_subq, s.true()) \
        .cte()
    join_product_info_query = join_product_info(join_board_product_subq).cte()

    product_cols = [(c.name, c) for c in join_product_info_query.c if 'board_id' not in c.name ]
    product_cols_json_agg = list(itertools.chain(*product_cols))
    group_by_board_subq = s.select(
            join_product_info_query.c.board_id,
            postgresql.array_agg(
                func.json_build_object(*product_cols_json_agg)
            ).label('products')
        ) \
        .group_by(join_product_info_query.c.board_id) \
        .cte()

    join_board_subq = s.select(p.Board.board_id, p.Board.name, p.Board.creation_date, p.Board.description, p.Board.artwork_url) \
        .join(user_board_ids_subq, user_board_ids_subq.c.board_id == p.Board.board_id) \
        .cte()
    join_board_type_subq = s.select(join_board_subq, p.BoardType) \
        .join(join_board_subq, join_board_subq.c.board_id == p.BoardType.board_id) \
        .cte()

    join_board_type_subq_cols = [c for c in join_board_type_subq.c if 'board_id' not in c.name ]
    join_board_info_and_products = s.select(*join_board_type_subq_cols, group_by_board_subq) \
        .outerjoin(group_by_board_subq, group_by_board_subq.c.board_id == join_board_type_subq.c.board_id)

    result = run_query(join_board_info_and_products)
    for board in result:
        if board['products'] is not None:
            board['products'] = sorted(board['products'], key=lambda k: k['last_modified_timestamp'], reverse=True)
    return {
            "boards": result
        }
