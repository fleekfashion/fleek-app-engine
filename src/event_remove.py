from src.utils import hashers
from src.defs import postgres as p

def remove_user_fave(conn, args):
    ## Required
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    product_id = args['product_id']

    query = p.USER_FAVES_TABLE.delete().where(p.USER_FAVES_TABLE.c.user_id == user_id).where(p.USER_FAVES_TABLE.c.product_id == product_id)
    print(query)

    conn = p.engine.connect()
    conn.execute(query)
    return True

def remove_user_bag(conn, args):
    ## Required
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    product_id = args['product_id']

    query = p.USER_BAGS_TABLE.delete().where(p.USER_BAGS_TABLE.c.user_id == user_id).where(p.USER_BAGS_TABLE.c.product_id == product_id)
    print(query)

    conn = p.engine.connect()
    conn.execute(query)
    return True