import sqlalchemy as s
from src.utils import board
from src.utils.sqlalchemy_utils import run_query
from src.utils import hashers
from src.defs import postgres as p

def getUserFaveProductBatch(args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    limit = args['limit']
    offset = args['offset']

    user_fave_pids_query = s.select(
        p.UserProductFaves.product_id,
        p.UserProductFaves.event_timestamp.label('last_modified_timestamp')
    ) \
        .filter(p.UserProductFaves.user_id == user_id) \
        .cte()

    products_batch_ordered = board.get_ordered_products_batch(
        user_fave_pids_query, 
        'last_modified_timestamp', 
        args
    ) \
        .limit(limit) \
        .offset(offset)
    
    result = run_query(products_batch_ordered)
    return {
        "products": result
    }

def getUserFaveStats(args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])

    user_fave_pids_query = s.select(
        s.literal('all_faves').label('board_id'), # Dummy value
        p.UserProductFaves.product_id,
        p.UserProductFaves.event_timestamp.label('last_modified_timestamp')
    ) \
        .filter(p.UserProductFaves.user_id == user_id) \
        .cte()
    get_product_group_stats_query = board.get_product_group_stats(user_fave_pids_query, None).cte()
    product_previews = board.get_product_previews(
        user_fave_pids_query, 
        'board_id',
        'last_modified_timestamp'
    ).cte()
    
    # Since both queries are one row, can merge into one db call
    join_queries = s.select(get_product_group_stats_query, product_previews).join(product_previews, s.true())

    result = run_query(join_queries)
    result = result[0] if len(result) > 0 else {}
    for dummy_key in ['temp_id', 'board_id']: result.pop(dummy_key, None)
    return {
        "stats_and_products": result
    }

def getUserBag(args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])

    user_bag_pids_query = s.select(
        p.UserProductBags.product_id,
        p.UserProductBags.event_timestamp.label('last_modified_timestamp')
    ) \
        .filter(p.UserProductBags.user_id == user_id) \
        .cte()

    products_batch_ordered = board.get_ordered_products_batch(
        user_bag_pids_query, 
        'last_modified_timestamp', 
        args
    )

    result = run_query(products_batch_ordered)
    return {
        "products": result
    }

def getUserFaveProductIds(args: dict) -> dict:
    """
    Returns a list of product ids
    for local sync with app
    """
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    limit = 3000

    q = s.select(
        p.UserProductFaves.product_id
    ).order_by(p.UserProductFaves.event_timestamp.desc()) \
        .limit(limit)
    result = run_query(q)
    res2 = [ r['product_id'] for r in result ]
    return {
        "product_ids": res2
    }
