import typing as t
import random

import sqlalchemy as s
from sqlalchemy.sql.selectable import Alias, CTE, Select
from sqlalchemy import subquery 
from sqlalchemy import Column
from sqlalchemy.orm.session import Session
from src.defs import postgres as p
from sqlalchemy.dialects import postgresql
from sqlalchemy import func as F
from sqlalchemy.sql.expression import literal, literal_column
from sqlalchemy.dialects.postgresql import array
from werkzeug.datastructures import ImmutableMultiDict

from src.utils import query as qutils 

from src.utils import user_info
from src.utils import static 

def get_product_group_stats(
        products: CTE,
        id_col: t.Optional[str]
    ) -> Select:

    ## Process id col
    id_colname = id_col if id_col else "temp_id"
    tmp_id_col = literal_column(id_col) if id_col else literal(1)


    ## join pinfo
    q3 = qutils.join_base_product_info(products).cte()

    ## Get advertiser level stats
    advertiser_stats = s.select(
        tmp_id_col.label('tmp_id'),
        q3.c.advertiser_name,
        F.count(q3.c.product_id).label('n_products')
    ).group_by(tmp_id_col, q3.c.advertiser_name) \
        .cte()

    ## Get total group level stats
    board_stats = s.select(
        advertiser_stats.c.tmp_id.label(id_colname),
        F.sum(advertiser_stats.c.n_products).label('n_products'),
        postgresql.array_agg(
            F.json_build_object(
                'advertiser_name', advertiser_stats.c.advertiser_name,
                'n_products', advertiser_stats.c.n_products
            )
        ).label('advertiser_stats'),
    ) \
        .group_by(advertiser_stats.c.tmp_id)

    return board_stats
