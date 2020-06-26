from src.utils.psycop_utils import cur_execute, get_labeled_values, get_columns

def get_single_product_info(conn, product_id):
    query = f"""
    SELECT *
    FROM product_info
    WHERE product_id={product_id}
    LIMIT 1
    """

    with conn.cursor() as cur:
        cur_execute(cur, query)
        columns = get_columns(cur)
        pinfo_tuple = cur.fetchone()
    
    if pinfo_tuple is None:
        return {}

    product_info = get_labeled_values(columns, pinfo_tuple)
    return product_info
