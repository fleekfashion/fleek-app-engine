import json
from src.utils import hashers
from src.defs import postgres as p
from src.utils.sqlalchemy_utils import session_scope

def upload_event(args) -> bool:
    new_args = {}

    new_args["event"] = args["event"]
    new_args["event_timestamp"] = args["event_timestamp"]
    new_args["user_id"] = hashers.apple_id_to_user_id_hash(args['user_id'])

    ## Optional
    new_args["method"] = args.get('method', None)
    new_args["product_id"] = args.get('product_id', None)
    new_args["tags"] = args.get("tags", [])
    new_args["advertiser_names"] = args.get("advertiser_names", None)
    new_args["product_labels"] = args.get("product_labels", None)
    new_args["searchstring"] = args.get("searchString", None)

    ## parse unstructured json data into string
    json_data = args.get("json_data", None)
    new_args["json_data"] = json.dumps(json_data) if json_data else None

    items = list(new_args.items())
    for key, value in items:
        if value is None: 
            new_args.pop(key)

    ue = p.UserEvents(**new_args)

    with session_scope() as session:
        session.add(ue)
        session.commit()
    return True
