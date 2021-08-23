from src.defs.types.product_group_by_type import ProductGroupByType
import sqlalchemy as s
from src.utils import board
from src.utils.sqlalchemy_utils import run_query
from src.utils import hashers
from src.defs import postgres as p
from sqlalchemy import func as F
from sqlalchemy.sql.selectable import Alias, CTE, Select
import typing as t

def _get_all_faves_grouped_product_previews_stmt(
    fave_pids_query: CTE, 
    group_by_field: str,
    ) -> Select:

    product_previews = board.get_product_previews(
        fave_pids_query, 
        group_by_field,
        'last_modified_timestamp'
    )

    num_products_query = s.select(
        fave_pids_query.c[group_by_field],
        F.count(fave_pids_query.c.product_id).label('n_products')
    ) \
        .group_by(fave_pids_query.c[group_by_field]) \
        .cte()

    final_product_previews = s.select(
        product_previews.c[group_by_field].label('name'),
        product_previews.c.products,
        num_products_query.c.n_products
    ) \
        .join(
            num_products_query, 
            product_previews.c[group_by_field] == num_products_query.c[group_by_field]
        ) \
        .order_by(product_previews.c[group_by_field])

    return final_product_previews


def getUserFaveProductBatch(args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    limit = args['limit']
    offset = args['offset']

    user_fave_pids_query = s.select(
        p.UserProductFaves.product_id,
        p.UserProductFaves.event_timestamp.label('last_modified_timestamp')
    ) \
        .filter(p.UserProductFaves.user_id == user_id) \
        .cte()

    products_batch_ordered = board.get_ordered_products_batch(
        user_fave_pids_query, 
        'last_modified_timestamp', 
        args
    ) \
        .limit(limit) \
        .offset(offset)
    
    result = run_query(products_batch_ordered)
    return {
        "products": result
    }

def getUserFaveStats(args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])

    user_fave_pids_query = s.select(
        s.literal('all_faves').label('board_id'), # Dummy value
        p.UserProductFaves.product_id,
        p.UserProductFaves.event_timestamp.label('last_modified_timestamp')
    ) \
        .filter(p.UserProductFaves.user_id == user_id) \
        .cte()
    get_product_group_stats_query = board.get_product_group_stats(user_fave_pids_query, None).cte()
    product_previews = board.get_product_previews(
        user_fave_pids_query, 
        'board_id',
        'last_modified_timestamp'
    ).cte()
    
    # Since both queries are one row, can merge into one db call
    join_queries = s.select(get_product_group_stats_query, product_previews).join(product_previews, s.true())

    result = run_query(join_queries)
    result = result[0] if len(result) > 0 else {}
    for dummy_key in ['temp_id', 'board_id']: result.pop(dummy_key, None)
    return {
        "stats_and_products": result
    }

def getUserBag(args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])

    user_bag_pids_query = s.select(
        p.UserProductBags.product_id,
        p.UserProductBags.event_timestamp.label('last_modified_timestamp')
    ) \
        .filter(p.UserProductBags.user_id == user_id) \
        .cte()

    products_batch_ordered = board.get_ordered_products_batch(
        user_bag_pids_query, 
        'last_modified_timestamp', 
        args
    )

    result = run_query(products_batch_ordered)
    return {
        "products": result
    }

def getUserFaveProductIds(args: dict) -> dict:
    """
    Returns a list of product ids
    for local sync with app
    """
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    limit = 3000

    q = s.select(
        p.UserProductFaves.product_id
    ) \
        .where(p.UserProductFaves.user_id == user_id) \
        .order_by(p.UserProductFaves.event_timestamp.desc()) \
        .limit(limit)
    result = run_query(q)
    res2 = [ r['product_id'] for r in result ]
    return {
        "product_ids": res2
    }

def getUserFaveProductsGrouped(args: dict) -> dict:
    user_id = hashers.apple_id_to_user_id_hash(args['user_id'])
    group_by_field = args.get('group_by_field', ProductGroupByType.ITEM_TYPE)

    if group_by_field == ProductGroupByType.ADVERTISER_NAME:
        user_fave_pids_query = s.select(
            p.UserProductFaves.product_id,
            p.ProductInfo.advertiser_name,
            p.UserProductFaves.event_timestamp.label('last_modified_timestamp')
        ) \
            .join(p.ProductInfo, p.UserProductFaves.product_id == p.ProductInfo.product_id) \
            .filter(p.UserProductFaves.user_id == user_id) \
            .cte()

        grouped_product_previews = _get_all_faves_grouped_product_previews_stmt(user_fave_pids_query, 'advertiser_name')
    else:
        user_fave_pids_query = s.select(
            p.UserProductFaves.product_id,
            F.unnest(p.ProductInfo.product_labels).label('item_type'),
            p.UserProductFaves.event_timestamp.label('last_modified_timestamp')
        ) \
            .join(p.ProductInfo, p.UserProductFaves.product_id == p.ProductInfo.product_id) \
            .filter(p.UserProductFaves.user_id == user_id) \
            .cte()

        grouped_product_previews = _get_all_faves_grouped_product_previews_stmt(user_fave_pids_query, 'item_type')

    result = run_query(grouped_product_previews)
    return {
        "faves": result
    }