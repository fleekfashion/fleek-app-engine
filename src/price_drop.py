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

def get_price_drop_board_insert_stmts(args: dict) -> List[Insert]:
    board_id = uuid.uuid4().hex
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    last_modified_timestamp = int(dt.now().timestamp())

    board_args = {
        'board_id': board_id,
        'creation_date': cast(dt.now().strftime('%Y-%m-%d'), s.Date),
        'name': args['board_name'],
        'description': args.get('description', None),
        'last_modified_timestamp': last_modified_timestamp,
        'artwork_url': args.get('artwork_url', None),
        'board_type': BoardType.PRICE_DROP
    }

    user_board_args = {
        'board_id': board_id,
        'user_id': user_id,
        'last_modified_timestamp': last_modified_timestamp,
        'is_owner': True,
        'is_collaborator': False,
        'is_following': False,
        'is_suggested': False,
    }
    
    get_boards_for_user_id_cte = s.select(p.Board.board_type) \
        .join(p.UserBoard, p.UserBoard.board_id == p.Board.board_id) \
        .where(p.UserBoard.user_id == user_id) \
        .cte()
    
    board_table = s.select(
        Values(
            s.column('board_id', UUID), 
            s.column('creation_date', s.Date), 
            s.column('name', s.Text), 
            s.column('description', s.Text), 
            s.column('artwork_url', s.Text), 
            s.column('last_modified_timestamp', s.BigInteger), 
            s.column('board_type', s.Text), 
            name='tmp'
        ).data([tuple(board_args.values())])
    ).cte()
    
    user_board_table = s.select(
        Values(
            s.column('board_id', UUID), 
            s.column('user_id', s.BigInteger), 
            s.column('last_modified_timestamp', s.BigInteger), 
            s.column('is_owner', s.Boolean), 
            s.column('is_collaborator', s.Boolean), 
            s.column('is_following', s.Boolean), 
            s.column('is_suggested', s.Boolean), 
            name='tmp'
        ).data([tuple(user_board_args.values())])
    ).cte()

    where_not_exists_stmt = ~s.exists([get_boards_for_user_id_cte.c.board_type]) \
        .where(get_boards_for_user_id_cte.c.board_type == BoardType.PRICE_DROP)
    
    board_select_stmt = s.select(board_table) \
        .where(where_not_exists_stmt) \
        .cte() 
    user_board_select_stmt = s.select(user_board_table) \
        .where(where_not_exists_stmt) \
        .cte()
    
    board_insert_statement = s.insert(p.Board).from_select(list(board_args.keys()) , board_select_stmt)
    user_board_insert_statement = s.insert(p.UserBoard).from_select(list(user_board_args.keys()) , user_board_select_stmt)

    return [board_insert_statement, user_board_insert_statement]
