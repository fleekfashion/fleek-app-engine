import typing as t
import itertools

import sqlalchemy as s
from src.utils import string_parser, board, query as qutils 
from sqlalchemy.sql.selectable import Alias, CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy import func as F 
from sqlalchemy.sql.expression import literal, literal_column


def getProducts(args: dict):
    user_id = hashers.apple_id_to_user_id_hash(args["user_id"])
    active_only : bool = args["active_only"].lower() == "true"
    offset = int(args['offset'])
    limit = int(args['limit'])

    board_id = args.get("board_id")
    smart_tag_id = args.get("smart_tag_id")

    order_by = args.get("order_by")

    if board_id is not None:
        q = s.select(
            p.BoardProduct.product_id,
            p.BoardProduct.last_modified_timestamp
        ).where(p.BoardProduct.board_id == board_id)
    elif smart_tag_id is not None:
        q = s.select(
            p.ProductSmartTag.product_id
        ).where(p.ProductSmartTag.smart_tag_id == smart_tag_id)
    else:
        return {
            "error": "must pass board id or smart tag id"
        }

    products = qutils.join_product_info(q.cte()).cte()
    filtered_products = qutils.apply_filters(products, args, active_only=active_only)

    if order_by is None:
        res = filtered_products
    elif order_by == "execution_date":
        res = filtered_products.order_by(
            *[
                literal_column("execution_date").desc(),
                literal_column('product_id').desc()
            ]
        )
    elif order_by == "last_modified_timestamp":
        res = filtered_products.order_by(
            *[
                literal_column("last_modified_timestamp").desc(),
                literal_column('product_id').desc()
            ]
        )
    elif order_by == "swipe_rate":
        res = filtered_products.order_by(
            *[
                literal_column('n_likes').desc(),
                literal_column('product_id').desc()
            ]
        )
    elif order_by == "personalized":
        res = qutils.apply_ranking(filtered_products.cte(), user_id, .8)
    elif order_by == "price_low":
        res = filtered_products.order_by(
            *[
                literal_column("product_sale_price").asc(),
                literal_column('product_id').desc()
            ]
        )
    elif order_by == "price_high":
        res = filtered_products.order_by(
            *[
                literal_column("product_sale_price").desc(),
                literal_column('product_id').desc()
            ]
        )

    results = run_query(res.offset(offset).limit(limit))

    return {
        "products": results
    }
