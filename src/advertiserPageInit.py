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


def advertiserPageInit(args: dict):
    user_id = hashers.apple_id_to_user_id_hash(args["user_id"])
    advertiser_name = args["advertiser_name"]

    products = s.select(p.ProductInfo) \
        .where(p.ProductInfo.is_active) \
        .where(p.ProductInfo.advertiser_name == advertiser_name) \
        .cte()

    n_products = s.select(
        F.count(products.c.product_id).label("n_products")
    ).cte()

    images = s.select(
        products.c.product_image_url
    ) \
        .order_by(products.c.n_likes.desc()) \
        .limit(5) \
        .cte()
    agg_images = s.select(
        psql.array_agg(images.c.product_image_url).label("top_brand_images")
    ).cte()

    q = s.select(
        n_products,
        agg_images
    ).join(
        agg_images, 
        n_products.c.n_products > (F.cardinality(agg_images.c.top_brand_images) - 10)
    )

    res = get_first(q)
    return res
