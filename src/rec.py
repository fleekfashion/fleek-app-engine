from random import shuffle

import psycopg2
from src.utils.psycop_utils import cur_execute, get_labeled_values, get_columns

def get_user_batch(conn, user_id):
    return 1

def increment_user_batch(conn, user_id):
    return 1

MIN_PRODUCTS = 30
PROB = 50
DELIMITER = ",_,"


def _arg_to_filter(arg, value):
    if arg == "min_price":
        return f"product_price > {value}"
    elif arg == "max_price":
        return f"product_price < {value}" ## ADD SALE PRICE
    elif arg == "advertiser_name":
        advertiser_ids = tuple(value.split(DELIMITER))
        return f"advertiser_name in {advertiser_ids}"
    elif arg == "product_tag":
        product_tags = tuple(value.split(DELIMITER))
        return f"product_tab in {product_tags}"
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
        
        query = f"SELECT * FROM user_product_recs WHERE user_id={user_id} AND batch={batch};"
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
        print(query)
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
    with conn.cursor() as cur:
        query = f" SELECT * FROM product_info TABLESAMPLE BERNOULLI({PROB}) {FILTER} LIMIT {n_products};"
        print(query)
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
    batch = get_user_batch(conn, user_id)
    FILTER = _build_filter(args)
    product_ids = get_user_product_ids(conn, user_id, batch=batch)
    products = []
    if len(product_ids) > 0:
        personalized_products = get_products_from_ids(conn, product_ids, FILTER=FILTER)
        products.extend(personalized_products)
    
    if batch > 0:
        n_rand = MIN_PRODUCTS - len(products)
        n_rand = max(n_rand, len(products)//3)
        rand_products = get_random_products(conn, n_rand)
        products = _random_merge(products, rand_products)
    increment_user_batch(user_id)
    return products
