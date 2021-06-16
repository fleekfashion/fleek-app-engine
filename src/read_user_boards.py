import typing as t

import sqlalchemy as s
from src.utils import query as qutils 
from sqlalchemy.sql.selectable import Alias, CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F 
import itertools

def _join_board_stats(q: CTE) -> Select:
    board_products = s.select(q.c.board_id, p.BoardProduct.product_id) \
        .filter(q.c.board_id == p.BoardProduct.board_id ) \
        .cte()
    q3 = qutils.join_base_product_info(board_products).cte()

    advertiser_stats = s.select(
        q3.c.board_id,
        q3.c.advertiser_name,
        F.count(q3.c.product_id).label('n_products')
    ).group_by(q3.c.board_id, q3.c.advertiser_name) \
        .cte()

    board_stats = s.select(
        advertiser_stats.c.board_id,
        F.sum(advertiser_stats.c.n_products).label('n_products'),
        postgresql.array_agg(
            F.json_build_object(
                'advertiser_name', advertiser_stats.c.advertiser_name,
                'n_products', advertiser_stats.c.n_products
            )
        ).label('advertiser_stats'),
    ) \
        .group_by(advertiser_stats.c.board_id) \
        .cte()

    return s.select(
        q,
        F.coalesce(board_stats.c.n_products, 0).label('n_products'),
        F.coalesce(board_stats.c.advertiser_stats, []).label('advertiser_stats')
    ).outerjoin(board_stats, q.c.board_id == board_stats.c.board_id)

def getBoardInfo(args: dict) -> dict:
    board_id = args['board_id']
    basic_board = s.select(p.Board).filter(p.Board.board_id == board_id).cte()
    board = _join_board_stats(basic_board)
    result = get_first(board)
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
    products_batch = qutils.join_product_info(board_pids_query) \
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

    board_product_lateral_subq = s.select(p.BoardProduct.board_id, p.BoardProduct.product_id) \
        .filter(p.BoardProduct.board_id == user_board_ids_subq.c.board_id) \
        .order_by(p.BoardProduct.last_modified_timestamp.desc()) \
        .limit(6) \
        .subquery() \
        .lateral()

    join_board_product_subq = s.select(board_product_lateral_subq, user_board_ids_subq.c.user_id) \
        .join(board_product_lateral_subq, s.true()) \
        .cte()
    join_product_info_query = qutils.join_product_info(join_board_product_subq).cte()

    product_cols = [(c.name, c) for c in join_product_info_query.c if 'board_id' not in c.name ]
    product_cols_json_agg = list(itertools.chain(*product_cols))
    group_by_board_subq = s.select(
            join_product_info_query.c.board_id,
            postgresql.array_agg(
                F.json_build_object(*product_cols_json_agg)
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

    join_board_type_subq_cols = [c for c in join_board_type_subq.c if 'board_id' not in c.name or 'board_id' == c.name ]
    join_board_info_and_products = s.select(*join_board_type_subq_cols, group_by_board_subq.c.products) \
        .outerjoin(group_by_board_subq, group_by_board_subq.c.board_id == join_board_type_subq.c.board_id)

    boards = _join_board_stats(join_board_info_and_products.cte())
    result = run_query(boards)
    return {
        "boards": result
    }
