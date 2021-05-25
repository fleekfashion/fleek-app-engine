import typing as t
from random import shuffle
from src.defs import postgres as p

from src.utils.psycop_utils import cur_execute, get_labeled_values, get_columns
from src.utils import static, hashers, user_info

import sqlalchemy as s
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.selectable import Alias, CTE, Select
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import literal
from sqlalchemy.sql import text
import src.utils.query as qutils
from src.utils.sqlalchemy_utils import row_to_dict, session_scope 


def getSimilarProducts(args: dict) -> t.List[dict]:
    product_id = args['product_id']
    offset = args.get('offset', 0)
    limit = args.get('limit', 50)

    with session_scope() as session:
        sim_pids = session.query(
            p.SimilarItems.similar_product_id.label('product_id')
        ).offset(offset) \
        .limit(limit) \
        .cte('similar_product_ids')
        similar_products = qutils.join_product_info(session, sim_pids)

    return [ row_to_dict(row) for row in similar_products.all() ]
