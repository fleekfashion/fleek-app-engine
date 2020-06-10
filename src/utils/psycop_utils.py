def cur_execute(cur, query, conn=None):
    try:
        cur.execute(query)

        if conn is not None:
            conn.commit()
    except Exception as e:
        print(e)
        cur.execute("ROLLBACK;")

def get_columns(cur):
    return [desc[0] for desc in cur.description]

def get_labeled_values(columns, values):
    ctov = dict( (c, v) for c, v in zip(columns, values))
    return ctov
