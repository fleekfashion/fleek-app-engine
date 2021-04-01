from src.utils.sqlalchemy_utils import session_scope
from src.utils import hashers
from src.defs import postgres as p
import uuid
from datetime import datetime as dt


def create_new_board(args: dict) -> dict:
    board_info_args = {}
    user_boards_args = {}
    board_type_args = {}
    
    board_id = uuid.uuid4().hex
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    last_modified_timestamp = int(dt.now().timestamp())
    creation_date = dt.now().strftime('%Y-%m-%d')

    ## Required fields
    board_info_args['board_id'] = board_id
    board_info_args['creation_date'] = creation_date
    board_info_args['last_modified_timestamp'] = last_modified_timestamp
    board_info_args['name'] = args['board_name']

    user_boards_args['user_id'] = user_id
    user_boards_args['board_id'] = board_id
    user_boards_args['last_modified_timestamp'] = last_modified_timestamp
    user_boards_args['is_owner'] = True
    user_boards_args['is_collaborator'] = False
    user_boards_args['is_following'] = False
    user_boards_args['is_suggested'] = False

    board_type_args['board_id'] = board_id
    board_type_args['is_smart'] = False
    board_type_args['is_price_drpo'] = False
    board_type_args['is_all_faves'] = False
    board_type_args['is_global'] = False
    board_type_args['is_daily_mix'] = False

    ## Optional fields
    board_info_args['description'] = args.get('description', None)
    board_info_args['artwork_url'] = args.get('artwork_url', None)

    ## Construct SQLAlchemy Objects
    board_info = p.BoardInfo(**board_info_args)
    user_board = p.UserBoards(**user_boards_args)
    board_products = [
        p.BoardProducts(board_id=board_id, product_id=product_id, last_modified_timestamp=last_modified_timestamp)
        for product_id in args.get('product_ids', [])
    ]

    ## Execute session transaction
    try:
        with session_scope() as session:
            session.add(board_info)
            session.add(user_board)
            session.add_all(board_products)
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True, "board_id": board_id}

def write_product_to_board(args: dict) -> dict:
    board_product_args = {}

    ## Required fields
    board_product_args['board_id'] = args['board_id']
    board_product_args['product_id'] = args['product_id']
    board_product_args['last_modified_timestamp'] = int(dt.now().timestamp())

    ## Construct SQLAlchemy Object
    board_product = p.BoardProducts(**board_product_args)

    ## Execute session transaction
    try:
        with session_scope() as session:
            session.add(board_product)
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True}

