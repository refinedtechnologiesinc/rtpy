# %% take raw table and assuming no name changes, make the stg tbl and dbt code
from sgs.db.snowflake.connect import connect
from sgs.db.dbt.craft import craft_dbt_model
from sgs.db.snowflake.describe import describe_table

# %% inputs
from_tbl = "stg_itrack__contracts"
to_tbl = "fct_mr_contracts"

db = "ANALYTICS"
schema = "STAGING"
warehouse = "TRANSFORMING"
role = "TRANSFORMER"

# %% Open connection
resp_conn = connect(warehouse=warehouse, db=db, schema=schema, role=role)
if resp_conn["status"] == 200:
    conn_raw = resp_conn["data"]

# %% get cols for the tbl
resp_tbl = describe_table(conn_raw, from_tbl)
if resp_tbl["status"] == 200:
    cols = resp_tbl["data"]

# %% # =============== make the dbt core model + sql
resp_stg = craft_dbt_model(
    from_schema=schema,
    from_tbl=from_tbl,
    cols=cols,
    to_tbl=to_tbl,
    subquery_alias="staging",
    output_alias="final",
    ref_src="ref",
)
if resp_stg["status"] != 200:
    raise ValueError(resp_stg)

with open(f"outputs/{to_tbl}.sql", "w") as file:
    file.write(resp_stg["data"]["dbt_sql"])
file.close()

with open(f"outputs/{to_tbl}.yml", "w") as file:
    file.write(resp_stg["data"]["model"])
file.close()

# %%
