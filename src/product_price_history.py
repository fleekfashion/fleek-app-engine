from random import shuffle

from src.utils.psycop_utils import cur_execute, get_labeled_values, get_columns

PRODUCT_PRICE_HISTORY_TABLE = 'prod.product_price_history'

def _get_product_price_history_query(product_id: int) -> str:
    return f"""
    SELECT EXECUTION_DATE, PRODUCT_PRICE
    FROM {PRODUCT_PRICE_HISTORY_TABLE}
    WHERE PRODUCT_ID = {product_id} 
    ORDER BY EXECUTION_DATE ASC
    """
    
def get_product_price_history(conn, args: dict) -> list:
    product_id = int(args['product_id'])
    query = _get_product_price_history_query(product_id)
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
