import sqlite3

from dsapplicationregistration import register
from common import ds_utils

def connect_to_db(db_name):
    conn = sqlite3.connect(db_name)
    return conn

def run_custom_query(query, db_name):
    conn = connect_to_db(db_name)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()

    # convert row objects to dictionary
    res = [dict(zip(row.keys(), row)) for row in rows]
    return res

@register()
def run_predefined_query():
    print("start testing SQL queries")
    files = ds_utils.get_all_files()
    db_name = files[0]
    query = "SELECT department, avg(salary) FROM info, payment " \
            "WHERE info.employee_id = payment.employee_id GROUP BY department"
    res = run_custom_query(query, db_name)
    return res
