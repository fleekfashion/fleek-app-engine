import sqlalchemy as s
from src.utils import board
from src.utils.sqlalchemy_utils import run_query
from src.utils import hashers
from src.defs import postgres as p


def getAllFavesProductBatch(args: dict) -> dict:
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