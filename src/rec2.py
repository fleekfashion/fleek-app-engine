from random import shuffle
from src.defs import postgres as p

from src.utils.psycop_utils import cur_execute, get_labeled_values, get_columns

MIN_PRODUCTS = 30
PROB = 50
DELIMITER = ",_,"

OUR_IDS = set(
        [
        1338143769388061356, # Naman
        1596069326878625953, # Kian
        182814591431031699, # Cyp
        1117741357120322720 # Kelly
    ]
)

def _arg_to_filter(arg: str, value) -> str:
    if arg == "min_price":
        return f"product_sale_price > {value}"
    if arg == "max_price":
        return f"product_sale_price < {value}"
    if arg == "advertiser_name":
        names = value+DELIMITER+"INVALID_NAME"
        advertiser_names = tuple(names.split(DELIMITER))
        return f"advertiser_name in {advertiser_names}"
    if arg == "product_tag":
        # TODO This is a hack. When there is only one item,
        # the tuple adds an unneccessary comma.
        tags = value.split(DELIMITER)
        tags = [ f"'{t}'" for t in tags ]
        tags = ", ".join(tags)
        labels = f"ARRAY[{tags}]"
        return f"product_labels && {labels}"
    if arg == "on_sale" and value:
            return "product_sale_price < product_price - 2"
    return ""

def _build_filter(args: dict) -> str:
    FILTER = "is_active"
    for k, v in args.items():
        f = _arg_to_filter(k, v)
        if len(f) != 0:
            FILTER += "\nAND " + f
    return FILTER



def _normalize_products_by_brand(table: str, limit: int):
    query = f"""
    SELECT * 
    FROM (
        SELECT t.*,
            random()*log(ac.n_products) as normalized_rank
        FROM {table} t
        INNER JOIN {p.ADVERTISER_PRODUCT_COUNT_TABLE.fullname} ac
        ON t.advertiser_name=ac.advertiser_name
    ) t2
    ORDER BY normalized_rank
    LIMIT {limit}
    """
    return query

def _get_personalized_products_query(user_id: int, FILTER: str, limit: int) -> str:
    return f"""
    SELECT pi.*
    FROM {p.PRODUCT_RECS_TABLE.fullname} pr
    INNER JOIN {p.PRODUCT_INFO_TABLE.fullname} pi 
      ON pr.product_id = pi.product_id
    WHERE user_id={user_id} AND {FILTER}
    ORDER BY pr.index
    LIMIT {limit} 
    """

def _get_random_products_query(FILTER, limit): 
    columns = p.PRODUCT_INFO_TABLE.get_columns()\
        .map(
            lambda x: f"pi.{x}"
        ).map(
            lambda x: x if 'product_tags' not in x
                else "array_append(product_tags, 'random_product') as product_tags"
        ).make_string(",\n")
    query = f"""
    (
        SELECT 
          {columns}
        FROM {p.PRODUCT_INFO_TABLE.fullname} pi
        TABLESAMPLE BERNOULLI (10)
        WHERE {FILTER}
    )
    """
    return _normalize_products_by_brand(query, limit=limit)

def _get_top_products_query(FILTER: str, limit: int) -> str:
    columns = p.PRODUCT_INFO_TABLE.get_columns()\
        .map(
            lambda x: f"pi.{x}"
        ).map(
            lambda x: x if 'product_tags' not in x
                else "array_append(product_tags, 'top_product') as product_tags"
        ).make_string(",\n")

    query = f"""
    (
        SELECT 
          {columns}
        FROM {p.PRODUCT_INFO_TABLE.fullname} pi
        INNER JOIN {p.TOP_PRODUCTS_TABLE.fullname} top_p 
          ON top_p.product_id = pi.product_id
        WHERE {FILTER}
    )
    """
    return _normalize_products_by_brand(query, limit=limit)

def _get_user_batch_query(FILTER: str, n_top: int, n_rand: int) -> str:
    return f"""
    WITH top_products AS (
        {_get_top_products_query(FILTER, n_top)}
    ),
    random_products AS (
        {_get_random_products_query(FILTER, n_rand)}
    ),
    products AS (
        SELECT * FROM (
            SELECT * FROM top_products 
                UNION
            SELECT * FROM random_products  
         ) t 
         ORDER BY RANDOM()
    ), psi AS (
        SELECT 
            product_id, 
            array_agg(row_to_json(t)) AS sizes
        FROM {p.PRODUCT_SIZE_INFO_TABLE.fullname} t
        WHERE product_id IN (
            SELECT product_id
            FROM products
        )
        GROUP BY product_id
    ), joined_products AS (
        SELECT 
            pi.*,
            psi.sizes
        FROM products pi 
        LEFT JOIN psi 
        ON pi.product_id = psi.product_id
    )

    SELECT *
    FROM joined_products 
    ORDER BY RANDOM()
    """

def _user_has_recs(conn, user_id: int) -> bool:
    ## Run Query
    query = f"SELECT * FROM {p.PRODUCT_RECS_TABLE.fullname} WHERE user_id={user_id} LIMIT 1;"
    with conn:
        with conn.cursor() as cur:
            cur_execute(cur, query)
            c = cur.rowcount
    return c > 0

def get_batch(conn, user_id: int, args: dict) -> list:
    FILTER = _build_filter(args)
    n_top = 30 if _user_has_recs(conn, user_id) else 15
    n_rand = 40 - n_top
    query = _get_user_batch_query(FILTER, n_top, n_rand)
    print(query)

    ## Run Query
    with conn:
        with conn.cursor() as cur:
            cur_execute(cur, query)
            columns = get_columns(cur)
            values = cur.fetchall()
    data = []
    for value in values:
        ctov = get_labeled_values(columns, value)
        data.append(ctov)
    return data

def get_similar_items(conn, product_id: int) -> list:
    query = f"""
    SELECT pi.*
    FROM {p.PRODUCT_INFO_TABLE.fullname} pi
    INNER JOIN 
    ( 
        SELECT similar_product_id AS product_id, index 
        FROM {p.SIMILAR_ITEMS_TABLE.fullname} si
        WHERE si.product_id={product_id} 
        ORDER BY index
        LIMIT 50
    ) si
    ON si.product_id = pi.product_id
    WHERE pi.is_active = true
    ORDER BY si.index
    """
    ## Run Query
    with conn.cursor() as cur:
        cur_execute(cur, query)
        columns = get_columns(cur)
        values = cur.fetchall()
    data = []
    for value in values:
        ctov = get_labeled_values(columns, value)
        data.append(ctov)
    return data
