import typing as t

import sqlalchemy as s
from src.utils import query as qutils 
from sqlalchemy.sql.selectable import Alias, CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F 
import itertools


def getAddToBoardOptions(args):
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    offset = int(args.get('offset', 0))
    limit = int(args.get('limit', 100))

    user_board_products = qutils._get_user_board_products(user_id, offset, limit, 1)
