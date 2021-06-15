from src.utils.sqlalchemy_utils import session_scope
from src.utils import hashers
from src.defs import postgres as p
import uuid
from datetime import datetime as dt


def create_new_board(args: dict) -> dict:
    board_id = uuid.uuid4().hex
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    last_modified_timestamp = int(dt.now().timestamp())
    creation_date = dt.now().strftime('%Y-%m-%d')
    board_name = args['board_name']

    ## Required fields
    board_args = {
        'board_id':board_id,
        'creation_date': creation_date,
        'last_modified_timestamp': last_modified_timestamp,
        'name': board_name,
    }

    user_board_args = {
        'user_id': user_id,
        'board_id': board_id,
        'last_modified_timestamp': last_modified_timestamp,
        'is_owner': True,
        'is_collaborator': False,
        'is_following': False,
        'is_suggested': False,
    }

    board_type_args = {
        'board_id': board_id,
        'is_user_generated': True,
        'is_smart': False,
        'is_price_drop': False,
        'is_all_faves': False,
        'is_global': False,
        'is_daily_mix': False,
    }

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
            session.commit()
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True, "board_id": board_id}

def write_product_to_board(args: dict) -> dict:
    board_product_args = {
        'board_id': args['board_id'],
        'product_id': args['product_id'],
        'last_modified_timestamp': int(dt.now().timestamp()),
    }

    ## Construct SQLAlchemy Object
    board_product = p.BoardProduct(**board_product_args)

    ## Execute session transaction
    try:
        with session_scope() as session:
            session.add(board_product)
            session.commit()
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True}
