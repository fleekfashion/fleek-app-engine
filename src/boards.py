from src.utils.sqlalchemy_utils import session_scope
from src.utils import hashers
from src.defs import postgres as p
import uuid
from datetime import datetime as dt

def create_new_board(args: dict) -> dict:
    board_info_args = {}
    user_boards_args = {}
    
    board_id = uuid.uuid4().hex
    last_modified_timestamp = int(dt.now().timestamp())

    ## Required fields
    board_info_args['board_id'] = board_id
    board_info_args['creation_date'] = dt.now().strftime('%Y-%m-%d')
    board_info_args['last_modified_timestamp'] = last_modified_timestamp
    board_info_args['name'] = args['board_name']

    user_boards_args['user_id'] = hashers.apple_id_to_user_id_hash(args['user_id'])
    user_boards_args['board_id'] = board_id
    user_boards_args['last_modified_timestamp'] = last_modified_timestamp
    user_boards_args['is_owner'] = True
    user_boards_args['is_collaborator'] = False
    user_boards_args['is_following'] = False
    user_boards_args['is_suggested'] = False

    ## Optional fields
    board_info_args['description'] = args.get('description', None)
    board_info_args['artwork_url'] = args.get('artwork_url', None)

    ## Construct SQLAlchemy Objects
    board_info = p.BoardInfo(**board_info_args)
    user_boards = p.UserBoards(**user_boards_args)

    ## Execute session transaction
    with session_scope() as session:
        session.add(board_info)
        session.add(user_boards)
    
    return {"success": True, "board_id": board_id}