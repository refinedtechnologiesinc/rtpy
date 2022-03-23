# %% take raw table and assuming no name changes, make the stg tbl and dbt code
from sgs.db.snowflake.connect import connect
from sgs.db.dbt.craft import craft_dbt_model

# %% inputs
schema = "itrack_fivetran"
raw_table = "itrackcontracts"
fct_dim = "dim"

# %% Open connection
resp_conn = connect("LOADING", "RAW", schema)
if resp_conn["status"] == 200:
    conn_raw = resp_conn["data"]

# %% =============== make the dbt stg model + sql
stg_table = f"stg_{schema}__{raw_table}"
resp_stg = craft_dbt_model(
    from_schema=schema,
    from_tbl=raw_table,
    conn=conn_raw,
    to_tbl=stg_table,
    subquery_alias="source",
    output_alias="staging",
)
if resp_stg["status"] != 200:
    raise ValueError(resp_stg)
#%% Write the outputs files
with open(f"{stg_table}.sql", "w") as file:
    file.write(resp_stg["data"]["dbt_sql"])
file.close()

with open(f"_{stg_table}_schema.yml", "w") as file:
    file.write(resp_stg["data"]["model"])
file.close()

cols = resp_stg["data"]["cols"]
# update the name to the AS name from the stg sql since when we pull from stg that'll be the actaul col name
for c in cols:
    c["Name"] = c.get("As", c["Name"])
# %% =============== make the dbt core model + sql
core_table = f"{fct_dim}_{raw_table}"
resp_core = craft_dbt_model(
    from_schema=schema,
    from_tbl=raw_table,
    cols=cols,
    to_tbl=core_table,
    subquery_alias="staging",
    output_alias="final",
)
if resp_core["status"] != 200:
    raise ValueError(resp_core)
# Write the outputs files
with open(f"{core_table}.sql", "w") as file:
    file.write(resp_core["data"]["dbt_sql"])
file.close()

with open(f"_{core_table}_schema.yml", "w") as file:
    file.write(resp_core["data"]["model"])
file.close()
