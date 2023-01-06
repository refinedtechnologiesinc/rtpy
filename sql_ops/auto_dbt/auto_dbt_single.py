# %% take raw table and assuming no name changes, make the stg tbl and dbt code
from sgs.db.snowflake.connect import connect
from sgs.db.dbt.craft import craft_dbt_model
from sgs.db.snowflake.describe import describe_table

# %% the current table to model to a downstram dbt sql
db = "RAW"
raw_schema = "SALESFORCE"
warehouse = "LOADING"
role = "LOADER"
raw_tbl = "COST_CROSS_REFERENCE_C"
stg_tbl = "stg_salesforce__cross_reference"
stg_schema = "SALESFORCE"
core_tbl = "fct_sf_products"


# %% Open connection
resp_conn = connect(warehouse=warehouse, db=db, schema=raw_schema, role=role)
if resp_conn["status"] == 200:
    conn_raw = resp_conn["data"]

# %% get cols for the tbl
resp_tbl = describe_table(conn_raw, raw_tbl)
if resp_tbl["status"] == 200:
    cols = resp_tbl["data"]

# %% # =============== make the dbt core model + sql
resp_stg = craft_dbt_model(
    from_schema=raw_schema,
    from_tbl=raw_tbl,
    cols=cols,
    to_tbl=stg_tbl,
    subquery_alias="source",
    output_alias="staging",
    ref_src="source",
)
if resp_stg["status"] != 200:
    raise ValueError(resp_stg)

with open(f"outputs/{stg_tbl}.sql", "w") as file:
    file.write(resp_stg["data"]["dbt_sql"])
file.close()

with open(f"outputs/{stg_tbl}.yml", "w") as file:
    file.write(resp_stg["data"]["model"])
file.close()


# %% # =============== make the dbt core model + sql
resp_stg = craft_dbt_model(
    from_schema=stg_schema,
    from_tbl=stg_tbl,
    cols=cols,
    to_tbl=core_tbl,
    subquery_alias="staging",
    output_alias="final",
    ref_src="ref",
)
if resp_stg["status"] != 200:
    raise ValueError(resp_stg)

with open(f"outputs/{core_tbl}.sql", "w") as file:
    file.write(resp_stg["data"]["dbt_sql"])
file.close()

with open(f"outputs/{core_tbl}.yml", "w") as file:
    file.write(resp_stg["data"]["model"])
file.close()

# %%
