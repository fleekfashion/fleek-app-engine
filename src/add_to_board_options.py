import typing as t

import sqlalchemy as s
from src.utils import query as qutils 
from sqlalchemy.sql.selectable import Alias, CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.sql.expression import literal
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F 
import itertools

from src.defs.types.board_type import BoardType

def getAddToBoardOptions(args):
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    product_id = int(args['product_id'])
    offset = int(args.get('offset', 0))
    limit = int(args.get('limit', 100))

    user_board_pid = qutils._get_user_board_products(user_id, offset, limit, 1).cte()
    user_board_products = qutils.join_base_product_info(user_board_pid).cte()

    user_board_info = qutils.join_board_info(
        s.select(p.UserBoard.board_id) \
            .where(p.UserBoard.user_id == user_id) \
            .where(p.UserBoard.is_owner | p.UserBoard.is_collaborator) \
            .cte()
    ).cte()

    q = s.select(
        user_board_info.c.board_id,
        user_board_info.c.name,
        user_board_products.c.product_image_url,
        literal(product_id).in_(
            s.select(p.BoardProduct.product_id) \
                .where(p.BoardProduct.board_id == user_board_info.c.board_id)
        ).label('is_in_board')
    ) \
        .where(user_board_info.c.board_type == BoardType.USER_GENERATED) \
        .outerjoin(user_board_products, user_board_info.c.board_id == user_board_products.c.board_id) \
        .order_by(user_board_info.c.last_modified_timestamp.desc())
    return run_query(q)
