import typing as t
import itertools
from functional import seq

import sqlalchemy as s
from sqlalchemy import Column
from src.utils import string_parser, board, query as qutils 
from sqlalchemy.sql.selectable import Alias, CTE, Select
from src.utils.sqlalchemy_utils import run_query, get_first 
from src.utils import hashers
from src.defs import postgres as p
from src.defs.types.board_type import BoardType
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy import func as F 
from sqlalchemy.sql.expression import literal, literal_column


def _load_new_products(advertiser_name: str) -> Select:
    pids = s.select(
        p.ProductInfo.product_id, 
        p.ProductInfo.execution_date,
        #literal(BoardType.ADVERTISER_NEW_PRODUCTS).label('board_type')
    ) \
        .where(p.ProductInfo.is_active) \
        .where(p.ProductInfo.advertiser_name == advertiser_name) \
        .order_by(p.ProductInfo.execution_date.desc(), p.ProductInfo.product_id) \
        .limit(1000)
    return pids

def _load_sale_products(advertiser_name: str) -> Select:
    pids = s.select(
        p.ProductInfo.product_id, 
        p.ProductInfo.execution_date,
        #literal(BoardType.ADVERTISER_SALE_PRODUCTS).label('board_type')
    ) \
        .where(p.ProductInfo.is_active) \
        .where(p.ProductInfo.advertiser_name == advertiser_name) \
        .where(p.ProductInfo.product_price > (p.ProductInfo.product_sale_price + 3)) \
        .order_by(p.ProductInfo.execution_date.desc(), p.ProductInfo.product_id) \
        .limit(1000)
    return pids

def _load_top_products(advertiser_name: str) -> Select:
    pids = s.select(
        p.ProductInfo.product_id,
        p.ProductInfo.execution_date,
        p.ProductInfo.n_likes,
        p.ProductInfo.n_views
        #literal(BoardType.ADVERTISER_SALE_PRODUCTS).label('board_type')
    ) \
        .where(p.ProductInfo.is_active) \
        .where(p.ProductInfo.advertiser_name == advertiser_name) \
        .where(p.ProductInfo.n_likes > 1) \
        .order_by(qutils.get_swipe_rate(), p.ProductInfo.product_id) \
        .limit(1000)
    return pids

def _get_board_object(pids: CTE, name: str, order_field: Column) -> Select:
    preview = board.get_product_previews(
        products=pids, 
        id_col=None, 
        order_field=order_field
    ).cte()
    stats = board.get_product_group_stats(pids, None).cte()

    q = s.select(
        preview.c.products,
        stats
    ) \
        .join(
            preview,
            stats.c.n_products > (F.cardinality(preview.c.products) - 10)
        ).cte()

    json_q = s.select(
        F.json_build_object(
            *(seq(q.c) \
                .flat_map(lambda c: [c.name, c ]) \
                .to_list() + [
                    "name", literal(name)
                ])
        ).label('board'),
        literal(0).label("stupid_int_col")
    )
    return json_q


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

    new_products_board = _get_board_object(
        _load_new_products(advertiser_name).cte(),
        f"Lastest Items from {advertiser_name}",
        literal_column("execution_date").desc()
    ).limit(1).cte()
    sale_products_board = _get_board_object(
        _load_sale_products(advertiser_name).cte(),
        f"On Sale at {advertiser_name}",
        literal_column("execution_date").desc()
    ).limit(1).cte()
    top_products_board = _get_board_object(
        _load_top_products(advertiser_name).cte(),
        f"Top Products at {advertiser_name}",
        qutils.get_swipe_rate()
    ).limit(1).cte()
    board_cols = [ c.name for c in new_products_board.c ]

    q = s.select(
        sale_products_board.c.board.label("advertiser_sale_products"),
        new_products_board.c.board.label("advertiser_new_products"),
        top_products_board.c.board.label("advertiser_top_products"),
        n_products,
        agg_images
    ).join(
        agg_images, 
        n_products.c.n_products > (F.cardinality(agg_images.c.top_brand_images) - 10)
    ).outerjoin(
        new_products_board,
        n_products.c.n_products > new_products_board.c.stupid_int_col
    ).outerjoin(
        sale_products_board,
        n_products.c.n_products > sale_products_board.c.stupid_int_col
    ).outerjoin(
        top_products_board,
        n_products.c.n_products > top_products_board.c.stupid_int_col
    ) 

    res = get_first(q)
    return res
