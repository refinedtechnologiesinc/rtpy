# %% take raw table and assuming no name changes, make the stg tbl and dbt code
from sgs.util.envi import _getvar
import sgs.db.snowflake.snowflake as sf
from sgs.db.snowflake.utility import execute
import pandas as pd

# %% inputs
schema = "rtidb_projects_dbo"

# %% Open connection
resp_c = sf.connect("LOADING", "RAW", schema)
if resp_c["status"] == 200:
    conn = resp_c["data"]

#%% get the tabels in this schema
stmt = f"describe schema {schema}"
resp_tbls = execute(stmt, conn=conn)
if resp_tbls["status"] == 200:
    tbls = resp_tbls["data"]

# %% get the columns from each table in the schema
column_data = []
for tbl in tbls:
    resp_ci = sf.get_columns(conn=conn, table=tbl[1], sort=True)
    if resp_ci["status"] != 200:
        raise ValueError(resp_c)
    for col in resp_ci["data"]:
        col["Schema"] = schema
        col["Table"] = tbl[1]
    column_data.extend(resp_ci["data"])
# %% export to csv
df = pd.DataFrame(data=column_data)
df.to_csv(f"describe_schema_{schema}.csv")
# %%
