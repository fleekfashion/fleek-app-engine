from src.utils.sqlalchemy_utils import session_scope, run_query
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects.postgresql import insert
import sqlalchemy as s
from sqlalchemy import func as F

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

def _add_product_event_batch_helper(event_table: p.PostgreTable, args: dict):
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    
    ## Filter out only valid product_ids 
    product_ids = [product['product_id'] for product in args['products']]
    filtered_product_ids_q = s.select(p.ProductInfo.product_id).where(p.ProductInfo.product_id == F.any(product_ids))
    filtered_product_ids_result = run_query(filtered_product_ids_q)
    filtered_product_ids = [product['product_id'] for product in filtered_product_ids_result]
    products_to_add = list(filter(lambda x: x['product_id'] in filtered_product_ids, args['products']))
    
    ## Construct event objects to be added
    def add_user_id_to_dict(d):
        d['user_id'] = user_id
        return d
    product_event_objects = list(map(add_user_id_to_dict, products_to_add))
    insert_product_event_statement = insert(event_table).values(product_event_objects).on_conflict_do_nothing()
    insert_product_seen_statement = insert(p.UserProductSeens).values(product_event_objects).on_conflict_do_nothing()

    try:
        with session_scope() as session:
            session.execute(insert_product_event_statement)
            session.execute(insert_product_seen_statement)
    except Exception as e:
        print(e)
        return False
    return True

def _remove_product_event_helper(event_table: p.PostgreTable, args: dict) -> bool:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    product_id = args['product_id']

    ## Execute session transaction
    remove_query = s.delete(event_table).where(
        s.and_(
            event_table.user_id == user_id,
            event_table.product_id == product_id
        )
    )
    try:
        with session_scope() as session:
            session.execute(remove_query)
    except Exception as e:
        print(e)
        return False
    return True

def write_user_product_seen(args: dict) -> bool:
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

def write_user_product_fave_batch(args: dict) -> bool:
    return _add_product_event_batch_helper(p.UserProductFaves, args)

def write_user_product_bag(args: dict) -> bool:
    return _add_product_event_helper(p.UserProductBags, args)

def write_user_product_bag_batch(args: dict) -> bool:
    return _add_product_event_batch_helper(p.UserProductBags, args)

def remove_user_product_fave(args: dict) -> bool:
    return _remove_product_event_helper(p.UserProductFaves, args)

def remove_user_product_bag(args: dict) -> bool:
    return _remove_product_event_helper(p.UserProductBags, args)