from src.utils.sqlalchemy_utils import load_session, row_to_dict, session_scope, table_row_to_dict
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func, select
import sqlalchemy as sqa
import itertools

def join_product_sizes(session, query):
    products_subquery = query.subquery(reduce_columns=True)
    sizes_subquery = session.query(
        p.ProductSizeInfo.product_id,
        postgresql.array_agg(
            func.json_build_object(
                'size', p.ProductSizeInfo.size,
                'product_purchase_url', p.ProductSizeInfo.product_purchase_url,
                'in_stock', p.ProductSizeInfo.in_stock,
            )
        ).label('sizes')
    ).filter(
        p.ProductSizeInfo.product_id.in_(session.query(products_subquery.c.product_id))
    ) \
     .group_by(p.ProductSizeInfo.product_id) \
     .subquery()

    return session.query(products_subquery, sizes_subquery.c.sizes) \
                  .join(sizes_subquery, sizes_subquery.c.product_id == products_subquery.c.product_id, isouter=True)

def join_product_info(session, product_id_query):
    subquery = product_id_query.subquery(reduce_columns=True)
    products_query = session.query(
            p.ProductInfo,
            subquery
        ).filter(subquery.c.product_id == p.ProductInfo.product_id)
    parsed_products_query = join_product_sizes(session, products_query)
    return parsed_products_query

def getBoardInfo(args: dict) -> dict:
    board_id = args['board_id']

    with session_scope() as session:
        board = session.query(p.Board).filter(p.Board.board_id == board_id).first()
        result = table_row_to_dict(board) if board else {'success': False}
    
    return result

def getBoardProductsBatch(args: dict) -> dict:
    board_id = args['board_id']
    offset = args['offset']
    limit = args['limit']

    with session_scope() as session:
        board_pids_query = session.query(p.BoardProduct.product_id) \
                        .filter(p.BoardProduct.board_id == board_id) \
                        .order_by(p.BoardProduct.last_modified_timestamp.desc())
        products_batch = join_product_info(session, board_pids_query) \
                        .limit(limit) \
                        .offset(offset) \
                        .all()
        result = [row_to_dict(row) for row in products_batch]
    
    return {
        "products": result
    }

def getUserBoardsBatch(args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    offset = args['offset']
    limit = args['limit']

    with session_scope() as session:
        user_board_ids_subq = session.query(p.UserBoard) \
                                .with_entities(p.UserBoard.board_id) \
                                .filter(p.UserBoard.user_id == user_id) \
                                .order_by(p.UserBoard.last_modified_timestamp.desc()) \
                                .offset(offset) \
                                .limit(limit) \
                                .subquery()


        board_product_lateral_subq = session.query(p.BoardProduct) \
                                        .with_entities(p.BoardProduct.board_id, p.BoardProduct.product_id) \
                                        .filter(p.BoardProduct.board_id == user_board_ids_subq.c.board_id) \
                                        .order_by(p.BoardProduct.last_modified_timestamp.desc()) \
                                        .limit(6) \
                                        .subquery() \
                                        .lateral()

        join_board_product_subq = session.query(user_board_ids_subq, board_product_lateral_subq) \
                                            .join(board_product_lateral_subq, sqa.true())

        join_product_info_query = join_product_info(session, join_board_product_subq).subquery()

        product_cols = [(c.name, c) for c in join_product_info_query.c]
        product_cols_json_agg = list(itertools.chain(*product_cols))
        group_by_board_subq = session.query(
                                    join_product_info_query.c.board_id,
                                    postgresql.array_agg(
                                        func.json_build_object(product_cols_json_agg)
                                    ).label('products')
                                ) \
                                .group_by(join_product_info_query.c.board_id) \
                                .subquery()


        join_board_subq = session.query(p.Board.board_id, p.Board.name, p.Board.creation_date, p.Board.description, p.Board.artwork_url) \
                                .join(user_board_ids_subq, user_board_ids_subq.c.board_id == p.Board.board_id) \
                                .subquery()

        join_board_type_subq = session.query(join_board_subq, p.BoardType) \
                                    .join(join_board_subq, join_board_subq.c.board_id == p.BoardType.board_id) \
                                    .subquery(reduce_columns=True)

        join_board_info_and_products = session.query(join_board_type_subq, group_by_board_subq) \
                                            .join(join_board_type_subq, group_by_board_subq.c.board_id == join_board_type_subq.c.board_id) \
                                            .all()

        result = [row_to_dict(row) for row in join_board_info_and_products]

    return result
                            
