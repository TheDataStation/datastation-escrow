import sqlite3

from dsapplicationregistration import register
from common import ds_utils

def run_custom_query(query, db_name):
    conn = sqlite3.connect(db_name)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()

    # convert row objects to dictionary
    res = [dict(zip(row.keys(), row)) for row in rows]
    return res

@register()
def predefined_on_DB():
    print("start running predefined query")
    files = ds_utils.get_all_files()
    db_name = files[0]
    query = "SELECT department, avg(salary) FROM info, payment " \
            "WHERE info.employee_id = payment.employee_id GROUP BY department"
    res = run_custom_query(query, db_name)
    return res

@register()
def customized_on_DB(user_query):
    print("start running user queries")
    files = ds_utils.get_all_files()
    db_name = files[0]
    res = run_custom_query(user_query, db_name)
    return res
