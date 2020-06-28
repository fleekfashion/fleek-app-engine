from src.utils.psycop_utils import cur_execute
from src.utils import hashers

USER_EVENTS_TABLE = "user_events" 

def upload_event(conn, args):
    args["method"] = args.get('method', '')
    args["product_id"] = args.get('product_id', '')
    args["user_id"] = hashers.apple_id_to_user_id_hash(args.get('user_id', 'no_user_id'))
    args["tags"] = args.get("tags", [])

    query = f"""
    INSERT INTO {USER_EVENTS_TABLE}
        (user_id, product_id, event_timestamp, event, method, tags)
    VALUES
        ( %(user_id)s, %(product_id)s, %(event_timestamp)s, %(event)s, %(method)s,  %(tags)s);
    """
    print(f"User Event:", args)
    with conn.cursor() as cur:
        cur_execute(cur, query, conn=conn, params=args)
    return True
