import typing as t
import time
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
from src.utils.sqlalchemy_utils import run_query 
import gc

def getSimilarProducts(args: dict) -> t.List[dict]:
    product_id = args['product_id']
    offset = args.get('offset', 0)
    limit = args.get('limit', 50)

    sim_pids = s.select(
        p.SimilarItems.similar_product_id.label('product_id')
    ) \
    .filter(p.SimilarItems.product_id == literal(product_id) ) \
    .offset(offset) \
    .limit(limit) \
    .cte('similar_product_ids')
    similar_products_query = qutils.join_product_info(sim_pids)
    return run_query(similar_products_query)
