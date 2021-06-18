from src.utils.sqlalchemy_utils import session_scope
from src.utils import hashers
from src.defs import postgres as p
import uuid
from datetime import datetime as dt
import sqlalchemy as s
from sqlalchemy.dialects.postgresql import insert


def create_new_board(args: dict) -> dict:
    board_id = uuid.uuid4().hex
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    last_modified_timestamp = int(dt.now().timestamp())
    creation_date = dt.now().strftime('%Y-%m-%d')
    board_name = args['board_name']
    board_type = args.get('board_type', 'user')

    ## Required fields
    board_args = {
        'board_id':board_id,
        'creation_date': creation_date,
        'last_modified_timestamp': last_modified_timestamp,
        'name': board_name,
        'board_type': board_type,
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

    ## Execute session transaction
    try:
        with session_scope() as session:
            session.add(board)
            session.add(user_board)
            session.add_all(board_products)
            session.commit()
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True, "board_id": board_id}

def write_product_to_board(args: dict) -> dict:
    timestamp = int(dt.now().timestamp())
    board_product_args = {
        'board_id': args['board_id'],
        'product_id': args['product_id'],
        'last_modified_timestamp': timestamp,
    }
    user_event_args = {
        'user_id': hashers.apple_id_to_user_id_hash(args['user_id']),
        'product_id': args['product_id'],
        'event_timestamp': timestamp
    }

    insert_event_statement = insert(p.UserProductFaves).values(**user_event_args).on_conflict_do_nothing()
    insert_product_seen_statement = insert(p.UserProductSeens).values(**user_event_args).on_conflict_do_nothing()

    ## Construct SQLAlchemy Object
    board_product = p.BoardProduct(**board_product_args)

    ## Execute session transaction
    try:
        with session_scope() as session:
            session.add(board_product)
            session.execute(insert_event_statement)
            session.execute(insert_product_seen_statement)
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True}

def remove_product_from_board(args: dict) -> dict:
    board_id = args['board_id']
    product_id = args['product_id']

    try:
        with session_scope() as session:
            remove_product_from_board_stmt = s.delete(p.BoardProduct).where(
                s.and_(
                    p.BoardProduct.board_id == board_id, 
                    p.BoardProduct.product_id == product_id
                )
            )
            session.execute(remove_product_from_board_stmt)
    except Exception as e:
        print(e)
        return {"success": False}

    return {"success": True}

def remove_board(args: dict) -> dict:
    board_id = args['board_id']
    try:
        with session_scope() as session:
            remove_board_statements = [
                s.delete(p.BoardProduct).where(p.BoardProduct.board_id == board_id),
                s.delete(p.UserBoard).where(p.UserBoard.board_id == board_id),
                s.delete(p.Board).where(p.Board.board_id == board_id)
            ]
            for statement in remove_board_statements: session.execute(statement)
    except Exception as e:
        print(e)
        return {"success": False}

    return {"success": True}

