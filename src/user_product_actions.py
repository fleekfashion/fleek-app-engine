from src.utils.sqlalchemy_utils import session_scope, run_query
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects.postgresql import insert
import sqlalchemy as s
from sqlalchemy import func as F
from sqlalchemy.sql import Values
from sqlalchemy.dialects.postgresql.dml import Insert
from sqlalchemy.sql.expression import literal

def _parse_product_event_args_helper(args: dict) -> dict:
    new_args = {}

    ## Required
    new_args['user_id'] = hashers.apple_id_to_user_id_hash(args['user_id'])
    new_args['product_id'] = args['product_id']
    new_args['event_timestamp'] = args['event_timestamp']

    return new_args

def build_write_to_smart_board_stmt(
        user_id: int, 
        product_id: int, 
        event_timestamp: int
    ) -> Insert:
    ## Get user Boards
    user_boards = s.select(p.UserBoard.board_id) \
        .where(p.UserBoard.user_id == user_id)

    ## Get smarttags for those boards
    user_board_smart_tags = s.select(p.BoardSmartTag) \
        .where(p.BoardSmartTag.board_id.in_(user_boards)) \
        .cte()

    ## Get relevent boards for the product
    board_product = s.select(
        user_board_smart_tags.c.board_id, 
        p.ProductSmartTag.product_id, 
        literal(event_timestamp).label('last_modified_timestamp')
    ) \
        .where(p.ProductSmartTag.product_id == product_id) \
        .join(user_board_smart_tags, p.ProductSmartTag.smart_tag_id == user_board_smart_tags.c.smart_tag_id) \
        .distinct()

    ## Write product to those boards
    insert_board_product= insert(p.BoardProduct) \
        .from_select(['board_id', 'product_id', 'last_modified_timestamp'], board_product) \
        .on_conflict_do_nothing()

    return insert_board_product

def _add_product_event_helper(event_table: p.PostgreTable, args: dict) -> bool:
    ## Parse args
    new_args = _parse_product_event_args_helper(args)

    ## Create objects
    insert_event_statement = insert(event_table).values(**new_args).on_conflict_do_nothing()
    insert_product_seen_statement = insert(p.UserProductSeens).values(**new_args).on_conflict_do_nothing()
    insert_to_smart_boards = build_write_to_smart_board_stmt(**new_args)

    ## Execute session transaction
    try:
        with session_scope() as session:
            session.execute(insert_event_statement)
            session.execute(insert_product_seen_statement)
            session.execute(insert_to_smart_boards)
            session.commit()
    except Exception as e:
        print(e)
        return False
    return True

def _get_insert_statement_for_bulk_upload(event_table: p.PostgreTable, args: dict) -> Insert:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    product_event_values_cte = s.select(
        Values(
            s.column('user_id', s.Integer), 
            s.column('product_id', s.Integer), 
            s.column('event_timestamp', s.Integer), 
            name='tmp'
        ).data([(user_id, product['product_id'], product['event_timestamp']) for product in args['products']])
    ).cte()

    filtered_product_event_q = s.select(product_event_values_cte) \
        .join(p.ProductInfo, product_event_values_cte.c.product_id == p.ProductInfo.product_id)
    insert_product_event_statement = insert(event_table) \
        .from_select(['user_id','product_id','event_timestamp'], filtered_product_event_q) \
        .on_conflict_do_nothing()
    
    return insert_product_event_statement


def _add_product_event_batch_helper(event_table: p.PostgreTable, args: dict):
    insert_product_event_statement = _get_insert_statement_for_bulk_upload(event_table, args)
    insert_product_seen_statement = _get_insert_statement_for_bulk_upload(p.UserProductSeens, args)

    try:
        with session_scope() as session:
            session.execute(insert_product_event_statement)
            session.execute(insert_product_seen_statement)
            session.commit()
    except Exception as e:
        print(e)
        return False
    return True

def _add_product_seen_batch_helper(args: dict):
    insert_product_seen_statement = _get_insert_statement_for_bulk_upload(p.UserProductSeens, args)

    try:
        with session_scope() as session:
            session.execute(insert_product_seen_statement)
            session.commit()
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
            session.commit()
    except Exception as e:
        print(e)
        return False
    return True

def _remove_product_event_batch_helper(event_table: p.PostgreTable, args: dict) -> bool:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    product_id_or_clause = s.or_(*[product_id == event_table.product_id for product_id in args['product_ids']])
    remove_query = s.delete(event_table) \
        .where(event_table.user_id == user_id) \
        .where(product_id_or_clause)

    try:
        with session_scope() as session:
            session.execute(remove_query)
            session.commit()
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
            session.commit()
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

def write_user_product_seen_batch(args: dict) -> bool:
    return _add_product_seen_batch_helper(args)

def remove_user_product_fave(args: dict) -> bool:
    return _remove_product_event_helper(p.UserProductFaves, args)

def remove_user_product_fave_batch(args: dict) -> bool:
    return _remove_product_event_batch_helper(p.UserProductFaves, args)

def remove_user_product_bag(args: dict) -> bool:
    return _remove_product_event_helper(p.UserProductBags, args)

def remove_user_product_bag_batch(args: dict) -> bool:
    return _remove_product_event_batch_helper(p.UserProductBags, args)
