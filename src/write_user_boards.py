from src.utils.sqlalchemy_utils import session_scope
from src.utils import hashers
from src.defs import postgres as p
import uuid
from datetime import datetime as dt
from sqlalchemy.dialects.postgresql import insert, UUID
import sqlalchemy as s
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import Values
from sqlalchemy.sql.expression import cast


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

def create_price_drop_board(args: dict) -> dict:
    board_id = uuid.uuid4().hex
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    last_modified_timestamp = int(dt.now().timestamp())
    creation_date = dt.now().strftime('%Y-%m-%d')
    board_name = args['board_name']

    ## Required fields
    board_args = {
        'board_id':board_id,
        'creation_date': creation_date,
        'name': board_name,
        'description': args.get('description', None),
        'last_modified_timestamp': last_modified_timestamp,
        'artwork_url': args.get('artwork_url', None),
        'board_type': 'price_drop',
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
    
    get_price_drop_boards_for_user_id_cte = s.select(p.Board.board_type) \
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
        ).data([(board_args['board_id'], cast(board_args['creation_date'], s.Date), board_args['name'], 
                 board_args['description'], board_args['last_modified_timestamp'], board_args['artwork_url'], 
                 board_args['board_type'])])
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
        ).data([(user_board_args['board_id'], user_board_args['user_id'], user_board_args['last_modified_timestamp'],
                user_board_args['is_owner'], user_board_args['is_collaborator'], user_board_args['is_following'],
                user_board_args['is_suggested'])])
    ).cte()
    
    board_select_stmt = s.select(board_table).where(
        ~s.exists([get_price_drop_boards_for_user_id_cte.c.board_type]) \
            .where(get_price_drop_boards_for_user_id_cte.c.board_type == 'price_drop')
    ).cte() 
    user_board_select_stmt = s.select(user_board_table).where(
        ~s.exists([get_price_drop_boards_for_user_id_cte.c.board_type]) \
            .where(get_price_drop_boards_for_user_id_cte.c.board_type == 'price_drop')
    ).cte()
    
    board_insert_statement = s.insert(p.Board).from_select(list(board_args.keys()) , board_select_stmt)
    user_board_insert_statement = s.insert(p.UserBoard).from_select(list(user_board_args.keys()) , user_board_select_stmt)

    ## Execute session transaction
    try:
        with session_scope() as session:
            session.execute(board_insert_statement)
            session.execute(user_board_insert_statement)
            session.commit()
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True}

def update_board_name(args: dict) -> dict:
    board_id = args['board_id']
    board_name = args['board_name']

    update_board_name_statement = s.update(p.Board) \
        .where(p.Board.board_id == board_id) \
        .values(name=board_name)

    try:
        with session_scope() as session:
            session.execute(update_board_name_statement)
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True}

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
    update_board_statement = s.update(p.Board) \
        .where(p.Board.board_id == board_product_args['board_id']) \
        .values(last_modified_timestamp=timestamp)

    ## Construct SQLAlchemy Object
    board_product = p.BoardProduct(**board_product_args)

    ## Execute session transaction
    try:
        with session_scope() as session:
            session.add(board_product)
            session.execute(insert_event_statement)
            session.execute(insert_product_seen_statement)
            session.execute(update_board_statement)
    except IntegrityError as e:
        return {"success": False, "error": "IntegrityError: Product already exists in this board."}
    except Exception as e:
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
            remove_board_tables = [p.BoardProduct, p.UserBoard, p.Board]
            remove_board_statements = [
                s.delete(table).where(table.board_id == board_id) for table in remove_board_tables
            ]
            for statement in remove_board_statements: session.execute(statement)
    except Exception as e:
        print(e)
        return {"success": False}

    return {"success": True}

