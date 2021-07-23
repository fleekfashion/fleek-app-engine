import typing as t
import itertools

import sqlalchemy as s
from sqlalchemy.sql.selectable import Alias, CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy import func as F 
from sqlalchemy.sql.expression import literal_column

from src.utils import query as qutils
from src.utils import hashers
from src.defs import postgres as p
from src.utils.sqlalchemy_utils import session_scope

def writeRejectSmartTagPopup(args: dict) -> bool:
    board_id = args['board_id']
    
    stmt = psql.insert(p.RejectedBoardSmartTagPopup) \
        .values(board_id=board_id) \
        .on_conflict_do_nothing()

    ## Execute session transaction
    try:
        with session_scope() as session:
            session.execute(stmt)
            session.commit()
    except Exception as e:
        print(e)
        return False
    return True
