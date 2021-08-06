import sqlalchemy as s
from src.utils import board
from src.utils.sqlalchemy_utils import run_query
from src.utils import hashers
from src.defs import postgres as p

def get_ordered_user_product_action_table_batch(table: s.Table, args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    limit = args.get('limit', None)
    offset = args.get('offset', 0)

    pids_query = s.select(
        table.product_id,
        table.event_timestamp.label('last_modified_timestamp')
    ) \
        .filter(table.user_id == user_id) \
        .cte()

    products_batch_ordered = board.get_ordered_products_batch(
        pids_query, 
        'last_modified_timestamp', 
        args
    ) \
        .limit(limit) \
        .offset(offset)
    
    result = run_query(products_batch_ordered)
    return {
        "products": result
    }

def getUserFaveProductBatch(args: dict) -> dict:
    return get_ordered_user_product_action_table_batch(p.UserProductFaves, args)

def getUserBag(args: dict) -> dict:
    return get_ordered_user_product_action_table_batch(p.UserProductBags, args)
    
def getUserFaveStats(args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])

    user_fave_pids_query = s.select(p.UserProductFaves.product_id) \
        .filter(p.UserProductFaves.user_id == user_id) \
        .cte()
    get_product_group_stats_query = board.get_product_group_stats(user_fave_pids_query, None)
    
    result = run_query(get_product_group_stats_query)
    return {
        "stats": result
    }
