from src.utils.sqlalchemy_utils import session_scope
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert
import sqlalchemy as s

def _parse_product_event_args_helper(args: dict) -> dict:
    new_args = {}

    ## Required
    new_args['user_id'] = hashers.apple_id_to_user_id_hash(args['user_id'])
    new_args['product_id'] = args['product_id']
    new_args['event_timestamp'] = args['event_timestamp']

    return new_args

def _add_product_event_helper(event_table: p.PostgreTable, args: dict) -> bool:
    ## Parse args
    new_args = _parse_product_event_args_helper(args)

    ## Create objects
    insert_event_statement = insert(event_table).values(**new_args).on_conflict_do_nothing()
    insert_product_seen_statement = insert(p.UserProductSeens).values(**new_args).on_conflict_do_nothing()

    ## Execute session transaction
    try:
        with session_scope() as session:
            session.execute(insert_event_statement)
            session.execute(insert_product_seen_statement)
    except Exception as e:
        print(e)
        return False
    return True

def _remove_product_event_helper(event_table: p.PostgreTable, args: dict) -> bool:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    product_id = args['product_id']

    ## Execute session transaction
    try:
        remove_query = s.delete(event_table).where(
            s.and_(
                event_table.user_id == user_id,
                event_table.product_id == product_id
            )
        )
        with session_scope() as session:
            session.execute(remove_query)
    except Exception as e:
        print(e)
        return False
    return True

def _add_product_seen_helper(args: dict) -> bool:
    ## Parse args
    new_args = _parse_product_event_args_helper(args)

    insert_product_seen_statement = insert(p.UserProductSeens).values(**new_args).on_conflict_do_nothing()

    try:
        with session_scope() as session:
            session.execute(insert_product_seen_statement)
    except Exception as e:
        print(e)
        return False
    return True

def write_user_product_fave(args: dict) -> bool:
    return _add_product_event_helper(p.UserProductFaves, args)

def write_user_product_bag(args: dict) -> bool:
    return _add_product_event_helper(p.UserProductBags, args)

def write_user_product_seen(args: dict) -> bool:
    return _add_product_seen_helper(args)

def remove_user_product_fave(args: dict) -> bool:
    return _remove_product_event_helper(p.UserProductFaves, args)

def remove_user_product_bag(args: dict) -> bool:
    return _remove_product_event_helper(p.UserProductBags, args)