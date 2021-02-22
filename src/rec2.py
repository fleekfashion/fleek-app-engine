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

def _arg_to_filter(arg, value):
    if arg == "min_price":
        return f"product_sale_price > {value}"
    elif arg == "max_price":
        return f"product_sale_price < {value}"
    elif arg == "advertiser_name":
        names = value+DELIMITER+"INVALID_NAME"
        advertiser_names = tuple(names.split(DELIMITER))
        return f"advertiser_name in {advertiser_names}"
    elif arg == "product_tag":
        # TODO This is a hack. When there is only one item,
        # the tuple adds an unneccessary comma.
        tags = value.split(DELIMITER)
        tags = [ f"'{t}'" for t in tags ]
        tags = ", ".join(tags)
        labels = f"ARRAY[{tags}]"
        return f"product_labels && {labels}"
    elif arg == "on_sale":
        if value:
            return "product_sale_price < product_price - 2"
    else:
        return ""

def _build_filter(args):
    FILTER = "is_active"
    for k, v in args.items():
        f = _arg_to_filter(k, v)
        if len(f) != 0:
            FILTER += "\nAND " + f
    return FILTER


def _get_personalized_products_query(user_id, FILTER, limit):
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
    return f"""
    SELECT 
      *
    FROM {p.PRODUCT_INFO_TABLE.fullname}
    WHERE {FILTER}
    ORDER BY RANDOM()
    LIMIT {limit} 
    """

def _get_top_products_query(FILTER, limit):
    return f"""
    SELECT pi.*
    FROM {p.PRODUCT_INFO_TABLE.fullname} pi
    INNER JOIN {p.TOP_PRODUCTS_TABLE.fullname} top_p 
      ON top_p.product_id = pi.product_id
    WHERE {FILTER}
    ORDER BY RANDOM()
    LIMIT {limit}
    """

def _get_new_user_batch_query(FILTER):
    return f"""
    CREATE TEMP TABLE top_products ON COMMIT DROP AS (
        {_get_top_products_query(FILTER, 30)}
    ); 
    CREATE TEMP TABLE random_products ON COMMIT DROP AS (
        {_get_random_products_query(FILTER, 10)}
    ); 

    UPDATE random_products
        SET product_tags = array_append(product_tags, 'random_product');

    SELECT * 
    FROM (
        SELECT * FROM top_products 
            UNION
        SELECT * FROM random_products  
        LIMIT 40
    ) p
    ORDER BY RANDOM();
    """

def _get_user_batch_query(user_id, FILTER):
    return f"""
    CREATE TEMP TABLE personalized_products ON COMMIT DROP AS (
        {_get_personalized_products_query(user_id, FILTER, 20)} 
    ); 
    CREATE TEMP TABLE random_products ON COMMIT DROP AS (
        {_get_random_products_query(FILTER, 30)}
    ); 
    CREATE TEMP TABLE top_products ON COMMIT DROP AS (
        {_get_top_products_query(FILTER, 5)}
    ); 

    UPDATE personalized_products
        SET product_tags = array_append(product_tags, 'personalized_product');
    UPDATE random_products
        SET product_tags = array_append(product_tags, 'random_product');

    SELECT * 
    FROM (
        SELECT * FROM personalized_products
            UNION
        SELECT * FROM top_products 
            UNION
        SELECT * FROM random_products  
        LIMIT 40
    ) p
    ORDER BY RANDOM();
    """

def _user_has_recs(conn, user_id):
    ## Run Query
    query = f"SELECT * FROM {p.PRODUCT_RECS_TABLE.fullname} WHERE user_id={user_id} LIMIT 1;"
    with conn:
        with conn.cursor() as cur:
            cur_execute(cur, query)
            c = cur.rowcount
    return c > 0

def get_batch(conn, user_id, args):
    FILTER = _build_filter(args)
    query = _get_user_batch_query(user_id, FILTER) if _user_has_recs(conn, user_id)\
            else _get_new_user_batch_query(FILTER)

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

def get_similar_items(conn, product_id):
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
