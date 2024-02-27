# %% take raw table and assuming no name changes, make the stg tbl and dbt code
from sgs.db.snowflake.connect import connect
from sgs.db.dbt.craft import craft_dbt_model
from sgs.db.snowflake.describe import describe_table
from sgs.util.envi import _getvar

# %% the current table to model to a downstram dbt sql
db = "RAW"
raw_schema = "RTIDB_DBO"
warehouse = "LOADING"
role = "LOADER"
tbl = "ADTermEmployee"

resp_conn = connect(warehouse=warehouse, db=db, schema=raw_schema, role=role)
if resp_conn["status"] == 200:
    conn_raw = resp_conn["data"]

#%%
resp_tbl = describe_table(conn_raw, tbl)
if resp_tbl["status"] == 200:
    cols = resp_tbl["data"]

# %%
