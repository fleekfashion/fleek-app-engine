import typing as t

import sqlalchemy as s
from src.utils import query as qutils 
from src.utils import board, string_parser
from sqlalchemy.sql.selectable import CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from sqlalchemy.sql.expression import literal_column
from src.defs import postgres as p
from src.defs.types.board_type import BoardType
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy import func as F 

def _get_board_smart_tags(board_ids: Select) -> Select:
    """
    Get table of (board_id, smart_tag_id)
    """
    board_smart_tags = s.select(
        p.BoardSmartTag.board_id,
        psql.array_agg(
            psql.aggregate_order_by(
                F.json_build_object(
                    "smart_tag_id", p.SmartTag.smart_tag_id,
                    "suggestion", p.SmartTag.suggestion,
                    "product_label", p.SmartTag.product_label
                ),
                p.SmartTag.suggestion
            )
        ).label('smart_tags')
    ) \
        .where(p.BoardSmartTag.board_id.in_(board_ids)) \
        .join(p.SmartTag, p.BoardSmartTag.smart_tag_id == p.SmartTag.smart_tag_id) \
        .group_by(p.BoardSmartTag.board_id)
    return board_smart_tags

def _get_board_opt_smart_tag(board_products: CTE) -> Select:
    """
    Get table of (board_id, smart_tag_id) with
    the max number of products in that board 
    """
    board_smart_tag = s.select(
        board_products.c.board_id,
        F.count(board_products.c.product_id).label('n_products'),
        p.ProductSmartTag.smart_tag_id
    ) \
    .join(p.ProductSmartTag, p.ProductSmartTag.product_id == board_products.c.product_id) \
    .where(~board_products.c.board_id.in_(s.select(p.BoardSmartTag.board_id))) \
    .where(~board_products.c.board_id.in_(s.select(p.RejectedBoardSmartTagPopup.board_id))) \
    .group_by(board_products.c.board_id, p.ProductSmartTag.smart_tag_id) \
    .cte()

    max_smart_tags = s.select(
        board_smart_tag.c.board_id,
        F.max(board_smart_tag.c.n_products).label('n_products')
    ).group_by(board_smart_tag.c.board_id)
    return max_smart_tags

def _get_boards_info(boards: CTE, user_id: t.Optional[int]) -> Select:
    """
    Get all relevant board metadata given a list of boards
    """
    board_ids = s.select(boards.c.board_id)
    board_products = s.select(p.BoardProduct.board_id, p.BoardProduct.product_id) \
        .filter(p.BoardProduct.board_id.in_(
            s.select(boards.c.board_id)
            )
        ) \
        .cte()
    board_stats = board.get_product_group_stats(board_products, 'board_id').cte()
    board_smart_tags = _get_board_smart_tags(board_ids).cte()
    board_opt_tag = _get_board_opt_smart_tag(board_products).cte()
    user_boards = s.select(p.UserBoard).where(p.UserBoard.user_id == user_id).cte()

    board_info = s.select(
        p.Board.__table__,
        F.coalesce(board_stats.c.n_products, 0).label('n_products'),
        F.coalesce(board_stats.c.advertiser_stats, []).label('advertiser_stats'),
        board_stats.c.total_savings,
        F.coalesce(board_smart_tags.c.smart_tags, []).label('smart_tags'),
        (
            (   1.0*F.coalesce(board_opt_tag.c.n_products, 0)
                / F.coalesce(board_stats.c.n_products, 1)
            ) > .5
        ).label('has_strong_suggestion'),
        F.json_strip_nulls(
            F.json_build_object(
                'user_id', p.UserProfile.user_id,
                'name', p.UserProfile.name,
                'profile_photo_url', p.UserProfile.profile_photo_url,
            )
        ).label('owner'),
        F.coalesce(user_boards.c.is_owner, False).label('is_owner'),
        F.coalesce(user_boards.c.is_collaborator, False).label('is_collaborator'),
        F.coalesce(user_boards.c.is_following, False).label('is_following'),
        user_boards.c.last_modified_timestamp.label('ub_last_modified_timestamp')
    ) \
        .where(p.Board.board_id.in_(board_ids)) \
        .outerjoin(board_stats, board_stats.c.board_id == p.Board.board_id) \
        .outerjoin(board_smart_tags, board_smart_tags.c.board_id == p.Board.board_id) \
        .outerjoin(board_opt_tag, board_opt_tag.c.board_id == p.Board.board_id) \
        .outerjoin(p.UserProfile, p.UserProfile.user_id == p.Board.owner_user_id) \
        .outerjoin(user_boards, user_boards.c.board_id == p.Board.board_id)

    return board_info

def getBoardInfo(args: dict) -> dict:
    board_id = args['board_id']
    user_id = args.get('user_id', None)
    user_id = hashers.apple_id_to_user_id_hash(user_id) if user_id else None
    basic_board = s.select(p.Board.board_id).filter(p.Board.board_id == board_id).cte()
    board = _get_boards_info(basic_board, user_id)
    result = get_first(board)
    parsed_res = result if result else {"error": "invalid collection id"}
    processed_board = string_parser.process_boards([parsed_res])[0]
    return processed_board

def getBoardProductsBatch(args: dict) -> dict:
    board_id = args['board_id']
    limit = args['limit']
    offset = args['offset']
    is_swipe_page = args.get('swipe_page', 'true').lower() == 'true'
    is_legacy = args.get('legacy', 'true').lower() == 'true'
    product_id_type = args.get('product_id_type', 'number')

    board_pids_query = s.select(
        p.BoardProduct.product_id, 
        p.BoardProduct.last_modified_timestamp
    ) \
        .filter(p.BoardProduct.board_id == board_id) \
        .cte()

    products_batch_ordered = board.get_ordered_products_batch(
        board_pids_query, 
        'last_modified_timestamp',
        args
    ) \
        .limit(limit) \
        .offset(offset)
    select_product_cols = qutils.select_product_fields(products_batch_ordered, is_swipe_page, is_legacy)

    result = run_query(select_product_cols)
    if product_id_type == 'string': result = string_parser.convert_product_ids_to_string(result)
    return {
        "products": result
    }


def getUserBoardsBatch(args: dict, dev_mode: bool = False) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id']) if not dev_mode else args['user_id']
    offset = args['offset']
    limit = args['limit']

    user_board_ids = s.select(p.UserBoard.board_id) \
        .filter(p.UserBoard.user_id == user_id)

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
    product_previews = board.get_product_previews(
            board_products,
            'board_id',
            literal_column('last_modified_timestamp').desc()
    ).cte()
    
    ## Join board info with the board products
    board_info = _get_boards_info(boards_batch, user_id).cte()
    boards = s.select(
            board_info,
            product_previews.c.products
        ) \
        .outerjoin(product_previews, product_previews.c.board_id == board_info.c.board_id) \
        .cte()

    boards_ordered = s.select(boards) \
            .where(
                s.or_(
                    boards.c.board_type == BoardType.USER_GENERATED,
                    F.cardinality(boards.c.products) > 0
                )
            ) \
            .order_by(F.greatest(boards.c.last_modified_timestamp, boards.c.ub_last_modified_timestamp).desc())
    result = run_query(boards_ordered)
    parsed_boards = string_parser.process_boards(result)
    
    return {
        "boards": parsed_boards 
    }
