from src.user_product_actions import get_insert_statement_for_bulk_upload
from src.utils.board import get_board_update_timestamp_statement
from src.defs.types.board_type import BoardType
from src.utils.sqlalchemy_utils import session_scope
from src.utils import hashers
from src.defs import postgres as p
import uuid
from datetime import datetime as dt
from sqlalchemy.dialects.postgresql import insert, UUID
import sqlalchemy as s
from sqlalchemy.exc import IntegrityError
from src import read_user_boards
from sqlalchemy.sql import Values

def create_new_board(args: dict) -> dict:
    board_id = uuid.uuid4().hex
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    last_modified_timestamp = int(dt.now().timestamp())
    creation_date = dt.now().strftime('%Y-%m-%d')
    board_name = args['board_name']
    board_type = args.get('board_type', BoardType.USER_GENERATED)

    ## Required fields
    board_args = {
        'board_id':board_id,
        'creation_date': creation_date,
        'last_modified_timestamp': last_modified_timestamp,
        'name': board_name,
        'board_type': board_type,
        'owner_user_id': user_id
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
    
    return {
        "success": True, 
        **read_user_boards.getBoardInfo({'board_id': board_id})
    }

def create_user_suggested_board(args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    smart_tag_id = args['smart_tag_id']
    board_name = args['board_name']
    board_id = uuid.uuid4().hex
    creation_date = dt.now().strftime('%Y-%m-%d')
    last_modified_timestamp = int(dt.now().timestamp())

    board_args = {
        'board_id': board_id,
        'creation_date': creation_date,
        'last_modified_timestamp': last_modified_timestamp,
        'name': board_name,
        'board_type': BoardType.USER_GENERATED,
        'owner_user_id': user_id
    }
    board = p.Board(**board_args)
    
    user_board_args = {
        'user_id': user_id,
        'board_id': board_id,
        'last_modified_timestamp': last_modified_timestamp,
        'is_owner': True,
        'is_collaborator': False,
        'is_following': False,
        'is_suggested': False,
    }
    user_board = p.UserBoard(**user_board_args)

    board_smart_tag_args = {
        'board_id': board_id,
        'smart_tag_id': smart_tag_id
    }
    board_smart_tag = p.BoardSmartTag(**board_smart_tag_args)

    product_ids_from_product_smart_tag = s.select(p.ProductSmartTag.product_id) \
        .filter(p.ProductSmartTag.smart_tag_id == smart_tag_id)

    product_ids_in_user_product_faves = s.select(
            p.UserProductFaves.product_id, 
            p.UserProductFaves.event_timestamp
        ) \
        .filter(p.UserProductFaves.user_id == user_id) \
        .filter(p.UserProductFaves.product_id.in_(product_ids_from_product_smart_tag)) \
        .cte()

    board_product_select_stmt = s.select(
        s.cast(s.literal(board_id).label('board_id'), UUID),
        product_ids_in_user_product_faves.c.product_id,
        product_ids_in_user_product_faves.c.event_timestamp.label('last_modified_timestamp'),
    ).cte()

    insert_board_product_stmt = insert(p.BoardProduct) \
        .from_select([col.name for col in board_product_select_stmt.c], board_product_select_stmt)

    try:
        with session_scope() as session:
            session.add(board)
            session.add(user_board)
            session.add(board_smart_tag)
            session.flush()
            session.execute(insert_board_product_stmt)
            session.commit()
    except Exception as e:
        print(e)
        return {"success": False}

    return {
        "success": True, 
        **read_user_boards.getBoardInfo({'board_id': board_id})
    }

def write_user_follow_board(args: dict) -> dict:
    board_id = args['board_id']
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    last_modified_timestamp = int(dt.now().timestamp())

    user_board_args = {
        'user_id': user_id,
        'board_id': board_id,
        'last_modified_timestamp': last_modified_timestamp,
        'is_owner': False,
        'is_collaborator': False,
        'is_following': True,
        'is_suggested': False,
    }

    user_board = p.UserBoard(**user_board_args)

    try:
        with session_scope() as session:
            session.add(user_board)
    except Exception as e:
        print(e)
        return {"success": False}

    return {"success": True}

def remove_user_follow_board(args: dict) -> dict:
    board_id = args['board_id']
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])

    remove_user_follow_stmt = s.delete(p.UserBoard) \
        .where(p.UserBoard.board_id == board_id) \
        .where(p.UserBoard.user_id == user_id)

    try:
        with session_scope() as session:
            session.execute(remove_user_follow_stmt)
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
    update_board_statement = get_board_update_timestamp_statement(board_product_args['board_id'], timestamp)

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

def write_product_batch_to_board(args: dict) -> dict:
    board_id = args['board_id']
    product_ids = args['product_ids']
    timestamp = int(dt.now().timestamp())

    ## Register product event
    product_event_args = {
        'user_id': args['user_id'],
        'products': [{'product_id': product_id, 'event_timestamp': timestamp} for product_id in product_ids]
    }

    insert_bulk_fave_statement  = get_insert_statement_for_bulk_upload(p.UserProductFaves, product_event_args)
    insert_bulk_seen_statement  = get_insert_statement_for_bulk_upload(p.UserProductSeens, product_event_args)
    update_board_statement = get_board_update_timestamp_statement(board_id, timestamp)

    ## Actually insert products
    insert_product_to_board_statements = [
        insert(p.BoardProduct) \
            .values(
                {
                    'board_id': board_id, 
                    'product_id': product_id, 
                    'last_modified_timestamp': timestamp
                }
            ) \
            .on_conflict_do_nothing()
        for product_id in product_ids
    ]

    try:
        with session_scope() as session:
            session.execute(insert_bulk_fave_statement)
            session.execute(insert_bulk_seen_statement)
            session.execute(update_board_statement)
            for stmt in insert_product_to_board_statements: session.execute(stmt)
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
            remove_board_tables = [p.BoardProduct, p.UserBoard, p.BoardSmartTag, p.Board]
            remove_board_statements = [
                s.delete(table).where(table.board_id == board_id) for table in remove_board_tables
            ]
            for statement in remove_board_statements: session.execute(statement)
    except Exception as e:
        print(e)
        return {"success": False}

    return {"success": True}

def write_smart_tag_to_board(args: dict) -> dict:
    board_smart_tag_args = {
        'board_id': args['board_id'],
        'smart_tag_id': args['smart_tag_id']
    }
    board_smart_tag = p.BoardSmartTag(**board_smart_tag_args)

    update_board_statement = get_board_update_timestamp_statement(
        board_smart_tag_args['board_id'], 
        int(dt.now().timestamp())
    )

    try:
        with session_scope() as session:
            session.add(board_smart_tag)
            session.execute(update_board_statement)
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True}

def remove_smart_tag_from_board(args: dict) -> dict:
    board_id = args['board_id']
    smart_tag_id = args['smart_tag_id']

    remove_smart_tag_from_board_stmt = s.delete(p.BoardSmartTag).where(
        s.and_(
            p.BoardSmartTag.board_id == board_id, 
            p.BoardSmartTag.smart_tag_id == smart_tag_id
        )
    )

    update_board_statement = get_board_update_timestamp_statement(
        board_id, 
        int(dt.now().timestamp())
    )

    try:
        with session_scope() as session:
            session.execute(remove_smart_tag_from_board_stmt)
            session.execute(update_board_statement)
    except Exception as e:
        print(e)
        return {"success": False}

    return {"success": True}
