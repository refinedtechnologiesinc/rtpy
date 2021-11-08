import pprint as pp


def get_columns(conn, schema, table):
    cur = conn.cursor()
    cur.execute(f"DESCRIBE {table}")
    cols = cur.fetchall()
    cur.close()
    cols = [c[0] for c in cols]
    pp.pprint(cols)
    return cols
