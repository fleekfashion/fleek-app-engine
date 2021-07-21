from src.utils.query import insert_on_where_not_exists_condition
from typing import List
import uuid
from datetime import datetime as dt

import sqlalchemy as s
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import Values
from sqlalchemy.sql.expression import cast

from src.defs import postgres as p
from src.defs.types.board_type import BoardType
from src.utils import hashers
from sqlalchemy.sql.dml import Insert


def _parse_board_args(args: dict, board_id: str, last_modified_timestamp: int, board_type: BoardType) -> dict:
    return {
        'board_id': board_id,
        'creation_date': cast(dt.now().strftime('%Y-%m-%d'), s.Date),
        'name': args['board_name'],
        'description': args.get('description', None),
        'last_modified_timestamp': last_modified_timestamp,
        'artwork_url': args.get('artwork_url', None),
        'board_type': BoardType.PRICE_DROP
    }

def _parse_user_board_args(args: dict, board_id: str, user_id: int, last_modified_timestamp: int) -> dict:
    return {
        'board_id': board_id,
        'user_id': user_id,
        'last_modified_timestamp': last_modified_timestamp,
        'is_owner': True,
        'is_collaborator': False,
        'is_following': False,
        'is_suggested': False,
    }

def get_insert_board_on_board_type_not_exists_statements(args: dict, board_type: BoardType) -> List[Insert]:
    board_id = uuid.uuid4().hex
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    last_modified_timestamp = int(dt.now().timestamp())
    
    board_args = _parse_board_args(args, board_id, last_modified_timestamp, board_type)
    user_board_args = _parse_user_board_args(args, board_id, user_id, last_modified_timestamp)
    
    get_boards_for_user_id_cte = s.select(p.Board.board_type) \
        .join(p.UserBoard, p.UserBoard.board_id == p.Board.board_id) \
        .where(p.UserBoard.user_id == user_id) \
        .cte()
    
    where_not_exists_stmt = ~s.exists([get_boards_for_user_id_cte.c.board_type]) \
        .where(get_boards_for_user_id_cte.c.board_type == board_type)

    board_insert_statement = insert_on_where_not_exists_condition(board_args, p.Board, where_not_exists_stmt)
    user_board_insert_statement = insert_on_where_not_exists_condition(user_board_args, p.UserBoard, where_not_exists_stmt)

    return [board_insert_statement, user_board_insert_statement]
