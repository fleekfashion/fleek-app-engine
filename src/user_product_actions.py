from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert

def _add_product_event_helper(table: p.PostgreTable, args: dict) -> bool:
    new_args = {}

    ## Required
    new_args['user_id'] = hashers.apple_id_to_user_id_hash(args['user_id'])
    new_args['product_id'] = args['product_id']
    new_args['event_timestamp'] = args['event_timestamp']

    ## Create insert statements
    product_event_insert_stmt = insert(table).values(**new_args).on_conflict_do_nothing()
    product_seen_insert_stmt = insert(p.USER_PRODUCT_SEENS_TABLE).values(**new_args).on_conflict_do_nothing()

    ## Compile inserts and convert to raw sql strings
    product_event_insert_str = str(product_event_insert_stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    product_seen_insert_str = str(product_seen_insert_stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    full_insert_str = product_event_insert_str + ";\n" + product_seen_insert_str + ";"

    ## Execute
    conn = p.engine.connect()
    conn.execute(full_insert_str, multi=True)
    return True

def _remove_product_event_helper(table: p.PostgreTable, args: dict) -> bool:
    ## Required
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    product_id = args['product_id']

    query = table.delete().where(table.c.user_id == user_id).where(table.c.product_id == product_id)

    conn = p.engine.connect()
    conn.execute(query)
    return True

def _add_product_seen_helper(args: dict) -> bool:
    new_args = {}

    ## Required
    new_args['user_id'] = hashers.apple_id_to_user_id_hash(args['user_id'])
    new_args['product_id'] = args['product_id']
    new_args['event_timestamp'] = args['event_timestamp']

    ## Create insert statement
    query = insert(p.USER_PRODUCT_SEENS_TABLE).values(**new_args).on_conflict_do_nothing()

    ## Execute
    conn = p.engine.connect()
    conn.execute(query)
    return True


def write_user_product_fave(args: dict) -> bool:
    return _add_product_event_helper(p.USER_PRODUCT_FAVES_TABLE, args)

def write_user_product_bag(args: dict) -> bool:
    return _add_product_event_helper(p.USER_PRODUCT_BAGS_TABLE, args)

def write_user_product_seen(args: dict) -> bool:
    return _add_product_seen_helper(args)

def remove_user_product_fave(args: dict) -> bool:
    return _remove_product_event_helper(p.USER_PRODUCT_FAVES_TABLE, args)

def remove_user_product_bag(args: dict) -> bool:
    return _remove_product_event_helper(p.USER_PRODUCT_BAGS_TABLE, args)