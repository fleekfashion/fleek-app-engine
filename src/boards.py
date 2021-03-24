from src.utils import hashers
from src.defs import postgres as p
import uuid
from datetime import datetime as dt

def create_new_board(args: dict) -> dict:
    new_args = {}

    ## Required
    new_args['board_id'] = uuid.uuid4().int >> 65
    new_args['creation_date'] = dt.now().strftime('%Y-%m-%d')
    new_args['last_modified_timestamp'] = dt.now()
    new_args['name'] = args['board_name']

    ## Optional
    new_args['description'] = args.get('description', None)
    new_args['artwork_url'] = args.get('artwork_url', None)

    ## Construct SQLAlchemy Object
    boardInfo = p.BoardInfo(**new_args)

    ## Commit insert
    session = p.load_session()
    session.add(boardInfo)
    session.commit()

    return {"success": True, "board_id": new_args['board_id']}