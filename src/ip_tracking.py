import typing as t

import sqlalchemy as s
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy import func as F

from src.utils.sqlalchemy_utils import session_scope
from src.utils import query as qutils
from src.defs import postgres as p
from src.utils.sqlalchemy_utils import get_first 

def upsertIPBoard(args: dict, ip_address: str) -> dict:
    board_id = args['board_id']
    timestamp = qutils.days_ago_timestamp(0)

    fields = dict(
        ip_address=ip_address,
        board_id=board_id,
        event_timestamp=timestamp
    )

    upsert_stmt = psql.insert(p.IP_BOARD) \
        .values(
            **fields
        ) \
        .on_conflict_do_update(
            index_elements=['ip_address'],
            set_=fields
        )

    try:
        with session_scope() as session:
            session.execute(upsert_stmt)
            session.commit()
    except Exception as e:
        print(e)
        return {
            "success": False
        }

    return {
        "success": True
    }

def getRecentIPBoard(args: dict, ip_address: str) -> dict:
    max_timestamp = qutils.days_ago_timestamp(1)

    q = s.select(
        p.IP_BOARD.board_id
    ) \
    .where(p.IP_BOARD.event_timestamp > max_timestamp) \
    .where(p.IP_BOARD.ip_address == ip_address)

    res = get_first(q)
    board_id = None if res is None else res['board_id']

    return {
        "board_id": board_id
    }

