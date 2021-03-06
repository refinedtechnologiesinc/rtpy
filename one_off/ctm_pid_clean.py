# %% take raw table and assuming no name changes, make the stg tbl and dbt code
import re
import logging
import sgs.db.snowflake as sf
from sgs.util.sql import execute
import pandas as pd

# %% Open connection
schema = "ctm"
resp_c = sf.connect("LOADING", "RAW", schema)
if resp_c['status'] == 200:
    conn_raw = resp_c['data']
    
# %% clean pid func
def clean_pid(pid):
    # everything after this assumes its a string
    if not isinstance(pid, str):
        # if empty
        if not pid or pd.isna(pid):
            return '9999'
        return int(pid)
    
    # if only numberic characters then continue
    if pid.isdecimal():
        return pid
    
    # one of their "SUM" summit pids
    if not isinstance(pid, str) and "SUM" in pid:
        return '9999'
    
    #* remove alpha chars
    _pid = re.sub("[a-zA-Z]+", "", pid)
    # if has a space split on it then find first
    # string trialing and leading whitespace
    _pid = _pid.strip()
    # if empty now
    if not _pid:
        return '9999'
    # if now just number
    if _pid.isdecimal():
        return _pid
    # find any breaks characters in the reaming string
    chars = [" ", "-", "/", ".", ","]
    for char in chars:
        if char in _pid:
            _pid_split = _pid.split(char)
            for p in _pid_split:
                if p.isdecimal():
                    return p
    # else set to 9999
    return '9999'
    
def update_pid(old, new, table, cursor):
    sql_u = f"""UPDATE {table}
                SET PID_NUMBER = '{new}'
                WHERE PID_NUMBER = '{old}';"""
    resp_ui = execute(sql_u, async_cursor=cursor)
    return resp_ui



# %% inputs
tables = ["creditcard_transactions", "bookings_air", "bookings_car", "bookings_hotel"]
# %% run
for table in tables:
    # %% Pull all records from table
    sql = f"Select PID_NUMBER from {schema}.{table} WHERE REGEXP_LIKE(PID_NUMBER, '.*?[^0-9].*');"
    resp_s = execute(sql, conn=conn_raw)
    if resp_s['status'] != 200:
        raise ValueError(resp_s)
    data = resp_s['data']
    logging.info(len(data))
    #%% open cursor to pass all update sql thru
    cursor = conn_raw.cursor()
    sfqids = []


    # %% get unique bad pids
    data = [d[0] for d in data]
    data = list(set(data))

    
    # %% for each pid 
    for d in data:
        resp_u = update_pid(d, clean_pid(d), f"{schema}.{table}",  cursor)
        if resp_u['status'] != 200:
            logging.error(resp_u['message'])
# %%

# %%
