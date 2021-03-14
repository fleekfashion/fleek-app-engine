from src.utils import hashers
from src.defs import postgres as p

def _add_brand_helper(table: p.PostgreTable, args: dict) -> bool:
    new_args = {}

    ## Required
    new_args['user_id'] = hashers.apple_id_to_user_id_hash(args['user_id'])
    new_args['advertiser_name'] = args['advertiser_name']
    new_args['event_timestamp'] = args['event_timestamp']

    query = table.insert(None).values(**new_args)
    conn = p.engine.connect()
    conn.execute(query)
    return True

def _remove_user_brand_helper(table: p.PostgreTable, args: dict) -> bool:
    ## Required
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    advertiser_name = args['advertiser_name']

    query = table.delete(None) \
            .where(p.USER_FAVED_BRANDS_TABLE.c.user_id == user_id) \
            .where(p.USER_FAVED_BRANDS_TABLE.c.advertiser_name == advertiser_name)

    conn = p.engine.connect()
    conn.execute(query)
    return True

def write_user_faved_brand(args: dict) -> bool:
    return _add_brand_helper(p.USER_FAVED_BRANDS_TABLE, args)

def write_user_muted_brand(args: dict) -> bool:
    return _add_brand_helper(p.USER_MUTED_BRANDS_TABLE, args)

def rm_user_faved_brand(args: dict) -> bool:
    return _remove_user_brand_helper(p.USER_FAVED_BRANDS_TABLE, args)

def rm_user_muted_brand(args: dict) -> bool:
    return _remove_user_brand_helper(p.USER_MUTED_BRANDS_TABLE, args)
