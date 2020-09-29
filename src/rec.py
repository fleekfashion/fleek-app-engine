from random import shuffle

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

PROJECT = "staging"
PRODUCT_INFO_TABLE = f"{PROJECT}.product_info"
TOP_PRODUCTS_TABLE = f"{PROJECT}.top_product_info"
SIMILAR_ITEMS_TABLE = f"{PROJECT}.similar_products"

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
            if len(FILTER) != 0:
                FILTER += "\nAND "
            FILTER += f
    return FILTER


def get_random_products(conn, n_products, top_products_only=False, FILTER=""):
    ## Create Query
    table = TOP_PRODUCTS_TABLE if top_products_only else PRODUCT_INFO_TABLE 
    sample = f"TABLESAMPLE BERNOULLI({PROB})" if top_products_only else ""
    query = f" SELECT * FROM {table} {sample}"
    query += "WHERE is_active=true\n"
    if len(FILTER) > 0:
        query += f" AND {FILTER}\n"
    query += "ORDER BY RANDOM()\n"
    query += f" LIMIT {n_products};"

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

def get_batch(conn, user_id, args):
    FILTER = _build_filter(args)
    products = get_random_products(conn, 30, FILTER=FILTER, top_products_only=False)
    return products 


def get_similar_items(conn, product_id):
    query = f"""
    SELECT pi.*
    FROM {PRODUCT_INFO_TABLE} pi
    INNER JOIN 
    ( 
        SELECT T.similar_product_id AS product_id, index 
        FROM {SIMILAR_ITEMS_TABLE} si,
            unnest(similar_product_ids) WITH ORDINALITY AS T (similar_product_id, index)
        WHERE si.product_id={product_id} 
        ORDER BY index
        LIMIT 20
    ) si
    ON si.product_id = pi.product_id
    WHERE pi.is_active = true
    LIMIT 10;
    """

    print(query)

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

