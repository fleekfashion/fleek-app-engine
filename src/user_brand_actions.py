from src.utils import hashers
from src.defs import postgres as p
from src.utils.user_info import pop_user_fave_brands 
from src.utils.sqlalchemy_utils import session_scope

import sqlalchemy as s
from sqlalchemy.orm.decl_api import DeclarativeMeta

def _add_brand_helper(table: DeclarativeMeta, args: dict) -> bool:
    new_args = {}
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])

    ## Required
    new_args['user_id'] = user_id
    new_args['advertiser_name'] = args['advertiser_name']
    new_args['event_timestamp'] = args['event_timestamp']

    with session_scope() as session:
        session.add(table(**new_args))
        session.commit()
    pop_user_fave_brands(user_id)
    return True

def _remove_user_brand_helper(table: DeclarativeMeta, args: dict) -> bool:
    ## Required
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    advertiser_name = args['advertiser_name']

    query = s.delete(table) \
        .where(table.user_id == user_id) \
        .where(table.advertiser_name == advertiser_name)

    with session_scope() as session:
        session.execute(query)
        session.commit()
    pop_user_fave_brands(user_id)
    return True

def write_user_faved_brand(args: dict) -> bool:
    _remove_user_brand_helper(p.UserMutedBrands, args) ## TODO Hack convert this to 1 call
    return _add_brand_helper(p.UserFavedBrands, args)

def write_user_muted_brand(args: dict) -> bool:
    _remove_user_brand_helper(p.UserFavedBrands, args)
    return _add_brand_helper(p.UserMutedBrands, args)

def rm_user_faved_brand(args: dict) -> bool:
    return _remove_user_brand_helper(p.UserFavedBrands, args)

def rm_user_muted_brand(args: dict) -> bool:
    return _remove_user_brand_helper(p.UserMutedBrands, args)
