from src.utils.psycop_utils import cur_execute
from src.utils import hashers

USER_EVENTS_TABLE = "user_events" 

def upload_event(conn, args):
    event = args.get('event', '')
    method = args.get('method', '')
    product_id = args.get('product_id', '')
    user_id = hashers.apple_id_to_user_id_hash(args.get('user_id', 'no_user_id'))
    event_timestamp = args.get('event_timestamp', '')
    
    query = f"""
    INSERT INTO {USER_EVENTS_TABLE}
        (user_id, product_id, event_timestamp, event, method)
    VALUES
        ( {user_id}, {product_id}, {event_timestamp}, '{event}', '{method}' );
    """
    print(query)
    with conn.cursor() as cur:
        cur_execute(cur, query, conn=conn)
    return True