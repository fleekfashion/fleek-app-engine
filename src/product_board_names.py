import typing as t
import itertools

import sqlalchemy as s
from sqlalchemy.sql.selectable import Alias, CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F 

from src.utils import query as qutils
from src.utils import hashers
from src.defs import postgres as p

def getProductBoardNameSuggestions(args: dict) -> dict:
    product_id = int(args['product_id'])

    q = s.select(
        p.SmartTag.smart_tag_id,
        p.SmartTag.suggestion
    ).where(p.SmartTag.smart_tag_id.in_(
        s.select(p.ProductSmartTag.smart_tag_id) \
            .where(p.ProductSmartTag.product_id == product_id)
        )
    ).order_by(p.SmartTag.n_hits.desc()) \
        .limit(6)

    res = run_query(q)

    return {
        'suggestions': res
    }
