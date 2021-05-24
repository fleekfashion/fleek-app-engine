
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

def getSingleProductInfo(args: dict) -> dict:
    product_id = args['product_id']

    with session_scope() as session:
        base_pinfo = session.query(
            p.ProductInfo
        ) \
        .where(p.ProductInfo.product_id == literal(product_id)) \
        .cte('base_product_info')
        pinfo = qutils.join_external_product_info(session, base_pinfo)
    return row_to_dict(pinfo.first()) 
