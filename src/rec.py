from random import shuffle

import psycopg2
from src.utils.psycop_utils import cur_execute, get_labeled_values, get_columns

MIN_PRODUCTS = 30
PROB = 50
DELIMITER = ",_,"

PRODUCT_INFO_TABLE = "product_info"
USER_PRODUCT_RECOMMENDATIONS_TABLE = "user_product_recommendations"
USER_BATCH_TABLE = "user_batch"

def get_user_batch(conn, user_id):
    query = f"""
    SELECT user_id, batch, last_filter
    FROM {USER_BATCH_TABLE}
    WHERE user_id = {user_id}
    """

    with conn.cursor() as cur:
        cur_execute(cur, query)
        values = cur.fetchone()
        if values is None:
            return {"batch": 1, "last_filter":"", "user_id": user_id}
        columns = get_columns(cur)
        data = get_labeled_values(columns, values)
    return data

def update_user_batch(conn, user_id, batch, last_filter):
    last_filter = last_filter.replace("'", "''")
    query = f"""
    UPDATE {USER_BATCH_TABLE}
    SET batch={batch}, last_filter='{last_filter}'
    WHERE user_id={user_id}
    """
    with conn.cursor() as cur:
        cur_execute(cur, query, conn=conn)

def _arg_to_filter(arg, value):
    if arg == "min_price":
        return f"product_price > {value}"
    elif arg == "max_price":
        return f"product_price < {value}" ## ADD SALE PRICE
    elif arg == "advertiser_name":
        names = value+DELIMITER+"INVALID_NAME"
        advertiser_names = tuple(names.split(DELIMITER))
        return f"advertiser_name in {advertiser_names}"
    elif arg == "product_tag":
        # TODO This is a hack. When there is only one item,
        # the tuple adds an unneccessary comma.
        tag = value+ DELIMITER + "INVALID_TAG"
        product_tags = tuple(tag.split(DELIMITER))
        return f"product_tag in {product_tags}"
    else:
        return ""

def _build_filter(args):
    FILTER = ""
    for k, v in args.items():
        f = _arg_to_filter(k, v)
        if len(f) != 0:
            if len(FILTER) != 0:
                FILTER += "\nAND "
            FILTER += f
    return FILTER

def get_user_product_ids(conn, user_id, batch):
    with conn.cursor() as cur:
        
        query = f"SELECT * FROM {USER_PRODUCT_RECOMMENDATIONS_TABLE} WHERE user_id={user_id} AND batch={batch};"
        cur_execute(cur, query)
        values = cur.fetchone()
        columns = get_columns(cur)
    
    if values is None:
        return ()
    ctov = get_labeled_values(columns, values)

    i = 0
    top_product_ids = []
    cname = f"top_products_{i}"
    while ctov.get(cname, 0):
        top_product_ids.append(ctov[cname])
        i+=1
        cname = f"top_products_{i}"
    return tuple(top_product_ids)

def get_products_from_ids(conn, product_ids, FILTER=""):
    with conn.cursor() as cur:
        query = f"SELECT * FROM product_info WHERE product_id in {product_ids}" 
        if len(FILTER) != 0:
            query += f"AND {FILTER};"
        cur_execute(cur, query)
        columns = get_columns(cur)
        values = cur.fetchall()
    
    data = []
    for value in values:
        ctov = get_labeled_values(columns, value)
        data.append(ctov)
    pid_to_ind = dict( zip( product_ids, range(len(product_ids))))
    data = sorted(data, key = lambda x: pid_to_ind[ x["product_id"]] )
    return data


def get_random_products(conn, n_products, FILTER=""):
    ## Create Query
    query = f" SELECT * FROM {PRODUCT_INFO_TABLE} TABLESAMPLE BERNOULLI({PROB})"
    if len(FILTER) > 0:
        query += f" WHERE {FILTER}\n"
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

def _random_merge(left, right):
    mid = ( len(left) + len(right) ) // 2
    left.extend(right)
    new_left, new_right = left[:mid], left[mid:]
    shuffle(new_right)
    res = []
    for l, r in zip(new_left, new_right):
        res.extend([l, r])
    return res
        
def get_batch(conn, user_id, args):
    FILTER = _build_filter(args)
    batch_data = get_user_batch(conn, user_id)
    batch = batch_data["batch"] if batch_data["last_filter"] == FILTER else 1
    product_ids = get_user_product_ids(conn, user_id, batch=batch)
    products = []
    if len(product_ids) > 0:
        personalized_products = get_products_from_ids(conn, product_ids, FILTER=FILTER)
        products.extend(personalized_products)

    if batch > 0:
        n_rand = MIN_PRODUCTS - len(products)
        n_rand = max(n_rand, len(products)//3)
        rand_products = get_random_products(conn, n_rand, FILTER=FILTER)
        products = _random_merge(products, rand_products)

    update_user_batch(conn, user_id, batch+1, last_filter=FILTER)
    return products
