from src.utils.sqlalchemy_utils import session_scope
from src.utils import hashers
from src.defs import postgres as p
import uuid
from datetime import datetime as dt


def create_new_board(args: dict) -> dict:
    board_args = {}
    user_board_args = {}
    board_type_args = {}
    
    board_id = uuid.uuid4().hex
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    last_modified_timestamp = int(dt.now().timestamp())
    creation_date = dt.now().strftime('%Y-%m-%d')

    ## Required fields
    board_args['board_id'] = board_id
    board_args['creation_date'] = creation_date
    board_args['last_modified_timestamp'] = last_modified_timestamp
    board_args['name'] = args['board_name']

    user_board_args['user_id'] = user_id
    user_board_args['board_id'] = board_id
    user_board_args['last_modified_timestamp'] = last_modified_timestamp
    user_board_args['is_owner'] = True
    user_board_args['is_collaborator'] = False
    user_board_args['is_following'] = False
    user_board_args['is_suggested'] = False

    board_type_args['board_id'] = board_id
    board_type_args['is_user_generated'] = True
    board_type_args['is_smart'] = False
    board_type_args['is_price_drop'] = False
    board_type_args['is_all_faves'] = False
    board_type_args['is_global'] = False
    board_type_args['is_daily_mix'] = False

    ## Optional fields
    board_args['description'] = args.get('description', None)
    board_args['artwork_url'] = args.get('artwork_url', None)

    ## Construct SQLAlchemy Objects
    board = p.Board(**board_args)
    user_board = p.UserBoard(**user_board_args)
    board_products = [
        p.BoardProduct(board_id=board_id, product_id=product_id, last_modified_timestamp=last_modified_timestamp)
        for product_id in args.get('product_ids', [])
    ]
    board_type = p.BoardType(**board_type_args)


    ## Execute session transaction
    try:
        with session_scope() as session:
            session.add(board)
            session.add(user_board)
            session.add(board_type)
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
    board_product = p.BoardProduct(**board_product_args)

    ## Execute session transaction
    try:
        with session_scope() as session:
            session.add(board_product)
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True}

