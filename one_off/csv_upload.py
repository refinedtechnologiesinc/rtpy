# %% take raw table and assuming no name changes, make the stg tbl and dbt code
from multiprocessing.sharedctypes import Value
import pandas as pd
from sgs.db.snowflake.connect import connect
from sgs.db.sql.utility import execute
from sgs.db.sql.write import df_upsert, df_to_upsert_sql

# %% inputs
to_tbl = "MR_COSTING"

db = "GENERATED"
schema = "ACCOUNTING"
warehouse = "LOADING"
role = "LOADER"

# %% Open connection
resp_conn = connect(warehouse=warehouse, db=db, schema=schema, role=role)
if resp_conn["status"] == 200:
    conn = resp_conn["data"]


# %% get the csv
path = "C:\\Users\\prest\\Documents\\"
df = pd.read_csv(path + to_tbl + ".csv")
df["total_variable"] = pd.to_numeric(df["total_variable"], errors="coerce")
df["total_fixed"] = pd.to_numeric(df["total_fixed"], errors="coerce")
df["item"] = (df.item.str.replace("'", "ft"))
df["item"] = (df.item.str.replace('"', "in"))

print(len(df))
# %% uplaod it
resp_u = df_upsert(df=df, table=to_tbl, pk="sku", conn=conn)

resp_u
# %%
upserts = df_to_upsert_sql(df, to_tbl, "sku")["data"]
for u in upserts:
    resp_ui = execute(u, conn=conn)
    if resp_ui["status"]!=200:
        raise ValueError(resp_ui)
# %%
