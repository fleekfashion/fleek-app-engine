from src.defs import postgres as p
from src.utils.psycop_utils import cur_execute, get_labeled_values, get_columns

def get_single_product_info(conn, product_id):
    query = f"""
    WITH psi AS (
        SELECT 
            product_id, 
            array_agg(row_to_json(t)) AS sizes
        FROM {p.PRODUCT_SIZE_INFO_TABLE.fullname} t
        WHERE product_id = {product_id}
        GROUP BY product_id
    ), joined_products AS (
        SELECT 
            pi.*,
            psi.sizes
        FROM {p.PRODUCT_INFO_TABLE.fullname} pi 
        LEFT JOIN psi 
        ON pi.product_id = psi.product_id
        WHERE pi.product_id = {product_id}
    )

    SELECT *
    FROM joined_products;
    """

    with conn.cursor() as cur:
        cur_execute(cur, query)
        columns = get_columns(cur)
        pinfo_tuple = cur.fetchone()
    
    if pinfo_tuple is None:
        return {}

    product_info = get_labeled_values(columns, pinfo_tuple)
    return product_info
