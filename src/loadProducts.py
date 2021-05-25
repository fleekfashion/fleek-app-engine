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


DELIMITER = ",_,"
FIRST_SESSION_FAVE_PCT = .9
FAVE_PCT= .55

def _user_has_recs(user_id: int) -> bool:
    ## Run Query
    with session_scope() as session:
        c = session.query(
            p.ProductRecs
        ).filter(p.ProductRecs.user_id == user_id) \
        .count()
    return c > 0


def process_products(
    session: Session,
    products_subquery: t.Union[Alias, CTE], 
    user_id: int,
    args: dict,
    limit: int,
    descr: str,
    ) -> Query:

    user_has_recs = _user_has_recs(user_id)
    is_first_session = args.get('is_first_session', False) or not user_has_recs

    filtered_products = qutils.apply_filters(
        session,
        products_subquery,
        args,
        active_only=True
    ).cte(f'filtered_products_{descr}')

    pct = FIRST_SESSION_FAVE_PCT if is_first_session else FAVE_PCT
    ranked_products = qutils.apply_ranking(session, filtered_products, user_id, pct) \
        .limit(limit)

    return ranked_products

def loadProducts(args: dict) -> list:

    user_id = args.get('user_id', -1)
    if user_id != -1:
        user_id = hashers.apple_id_to_user_id_hash(user_id)
    user_has_recs = _user_has_recs(user_id)
    n_top = 5 if user_has_recs else 15
    n_rand = 30 - n_top

    with session_scope() as session:

        top_products = qutils.join_base_product_info(
            session,
            session.query(p.TopProducts).cte('top_product_ids'),
        ).cte('top_product_info')

        sampled_products = session.query(
            s.tablesample(p.ProductInfo, s.func.bernoulli(3))
        ).cte("sampled_products")

        final_top_products = process_products(
            session,
            top_products,
            user_id,
            args,
            n_top,
            "top_products"
        ).cte('final_top_products')

        final_random_products = process_products(
            session,
            sampled_products,
            user_id,
            args,
            n_top,
            "random_products"
        ).cte('final_random_products')

        all_products = qutils.union_by_names(
            session,
            final_top_products,
            final_random_products
        ).cte('all_products')

        complete_products_query = qutils.join_external_product_info(
            session,
            all_products
        )

    return [ row_to_dict(row) for row in complete_products_query.all() ]


def getProductColorOptions(args: dict) -> dict:
    product_id = args['product_id']

    with session_scope() as session:
        ## Get alt colors
        alt_pids = session.query(
            p.ProductColorOptions.alternate_color_product_id.label('product_id')
        ) \
            .filter(p.ProductColorOptions.product_id == product_id)

        ## Add current pid
        all_pids = alt_pids.union(
            session.query(
                literal(product_id).label('product_id')
            )
        ).subquery(reduce_columns=True)

        ## Get product info
        pinfo_subq = qutils.join_product_info(
            session,
            all_pids
        ).cte()

        ## Order products
        order_products = session.query(pinfo_subq) \
            .filter(pinfo_subq.c.is_active == True) \
            .order_by(pinfo_subq.c.color).all()

    products = [ row_to_dict(row)  for row in order_products ]
    return {
        'products': products
    }
