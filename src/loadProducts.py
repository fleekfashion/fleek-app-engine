from random import shuffle
from src.defs import postgres as p

from src.utils.psycop_utils import cur_execute, get_labeled_values, get_columns
from src.utils import user_info
from src.utils import static 

import sqlalchemy as s
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

def loadProducts(args: dict) -> list:

    user_id = args.get('user_id', -1)
    user_has_recs = _user_has_recs(user_id)
    is_first_session = args.get('is_first_session', False) or not user_has_recs
    n_top = 5 if user_has_recs else 25
    n_rand = 40 - n_top

    with session_scope() as session:

        top_products = qutils.join_base_product_info(
            session,
            session.query(p.TopProducts).cte('top_product_ids'),
        ).cte('top_product_info')

        sampled_products = session.query(
            s.tablesample(p.ProductInfo, s.func.bernoulli(3))
        ).cte("sampled_products")

        filtered_products = qutils.apply_filters(
            session,
            sampled_products,
            args,
            active_only=True
        ).cte('filtered_products')

        pct = FIRST_SESSION_FAVE_PCT if is_first_session else FAVE_PCT
        ranked_random_products = qutils.apply_ranking(session, filtered_products, user_id, pct) \
            .limit(n_rand) \
            .cte('ranked_random_products')


        complete_products_query = qutils.join_external_product_info(
            session,
            ranked_random_products
        )
    return [ row_to_dict(row) for row in complete_products_query.all() ]















