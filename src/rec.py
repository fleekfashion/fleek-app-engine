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

PRODUCT_INFO_TABLE = "product_info"
TOP_PRODUCTS_TABLE = "top_product_info"
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
            return {"batch": None, "last_filter":"", "user_id": user_id}
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
        tag = value+ DELIMITER + "INVALID_TAG"
        product_tags = tuple(tag.split(DELIMITER))
        return f"product_tag in {product_tags}"
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
        query = f"SELECT * FROM {PRODUCT_INFO_TABLE} WHERE product_id in {product_ids}" 
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
    PRODUCTS = []
    FILTER = _build_filter(args)
    batch_data = get_user_batch(conn, user_id)
    batch = batch_data["batch"] if batch_data["last_filter"] == FILTER or batch_data["batch"] is None else 1

    ## New user: use top products
    if batch is None:
        N_TOP = 30
        top_products = get_random_products(conn, N_TOP, FILTER=FILTER, top_products_only=True)
        for p in top_products:
            p["tags"] = ["top_product"]
        PRODUCTS.extend(top_products)
    else:
        ## Get Personalized Products
        product_ids = get_user_product_ids(conn, user_id, batch=batch)
        if len(product_ids) > 0:
            personalized_products = get_products_from_ids(conn, product_ids, FILTER=FILTER)
            for p in personalized_products:
                p["tags"] = ["personalized_product"]
            PRODUCTS.extend(personalized_products)
            
        ## Add personalized tag
        if user_id in OUR_IDS:
            for p in PRODUCTS:
                p["product_name"] += " PERSONALIZED"

        update_user_batch(conn, user_id, batch+1, last_filter=FILTER)

    ## Add random products.
    ## Fill to minimum batchsize or add 1/2 of # personalized.
    #
    n_rand = max(MIN_PRODUCTS - len(PRODUCTS), len(PRODUCTS)//2)
    rand_products = get_random_products(conn, n_rand, FILTER=FILTER)
    for p in rand_products:
        p["tags"] = ["random_product"]
    PRODUCTS = _random_merge(PRODUCTS, rand_products)
    return PRODUCTS
