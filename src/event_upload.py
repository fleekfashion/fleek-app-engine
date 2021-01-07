from src.utils.psycop_utils import cur_execute
from src.utils import hashers

USER_EVENTS_TABLE = "prod.user_events"

def upload_event(conn, args):
    ## Required
    args["event"] = args["event"]
    args["event_timestamp"] = args["event_timestamp"]

    ## Optional
    args["method"] = args.get('method', None)
    args["product_id"] = args.get('product_id', None)
    args["user_id"] = hashers.apple_id_to_user_id_hash(args.get('user_id', 'no_user_id'))
    args["tags"] = args.get("tags", [])
    args["advertiser_names"] = args.get("advertiser_names", None)
    args["product_labels"] = args.get("product_labels", None)
    args["searchString"] = args.get("searchString", None)

    query = f"""
    INSERT INTO {USER_EVENTS_TABLE}
        (user_id, product_id, event_timestamp, event, method, tags, advertiser_names, product_labels, searchString)
    VALUES
        ( %(user_id)s, %(product_id)s, %(event_timestamp)s, %(event)s, %(method)s,  %(tags)s, %(advertiser_names)s, %(product_labels)s, %(searchString)s);
    """
    print(f"User Event:", args)
    with conn.cursor() as cur:
        cur_execute(cur, query, conn=conn, params=args)
    return True
