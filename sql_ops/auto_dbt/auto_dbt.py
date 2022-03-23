# %% take raw table and assuming no name changes, make the stg tbl and dbt code
import os
from sgs.db.snowflake.connect import connect
from sgs.db.dbt.craft import craft_dbt_model
from sgs.db.sql.describe import describe_schema

# %% inputs
schema = "itrack_fivetran"
fct_dim = "fct"

# %% Open connection
resp_conn = connect("LOADING", "RAW", schema)
if resp_conn["status"] == 200:
    conn_raw = resp_conn["data"]

# %% get all tables in schma
resp_ds = describe_schema(conn_raw, schema)
if resp_ds["status"] == 200:
    tbls = resp_ds["data"]

# %%
for tbl in tbls:
    # =============== make the dbt stg model + sql
    stg_table = f"stg_{schema}__{tbl['Name']}"
    stg_table = stg_table.lower()
    resp_stg = craft_dbt_model(
        from_schema=schema,
        from_tbl=tbl["Name"],
        conn=conn_raw,
        to_tbl=stg_table,
        subquery_alias="source",
        output_alias="staging",
    )
    if resp_stg["status"] != 200:
        raise ValueError(resp_stg)

    # Write the outputs files
    isExist = os.path.exists(f"./{schema}")

    if not isExist:
        # Create a new diretory because it does not exist
        os.makedirs(f"./{schema}")

    with open(f"{schema}/{stg_table}.sql", "w") as file:
        file.write(resp_stg["data"]["dbt_sql"])
    file.close()

    with open(f"{schema}/{stg_table}.yml", "w") as file:
        file.write(resp_stg["data"]["model"])
    file.close()

    # =============== make the dbt core model + sql
    cols = resp_stg["data"]["cols"]
    # update the name to the AS name from the stg sql since when we pull from stg that'll be the actaul col name
    for c in cols:
        c["Name"] = c.get("As", c["Name"])

    core_table = f"{fct_dim}_{tbl['Name']}"
    core_table = core_table.lower()
    resp_core = craft_dbt_model(
        from_schema=schema,
        from_tbl=tbl["Name"],
        cols=cols,
        to_tbl=core_table,
        subquery_alias="staging",
        output_alias="final",
    )
    if resp_core["status"] != 200:
        raise ValueError(resp_core)

    # Write the outputs files
    isExist = os.path.exists("./core")

    if not isExist:
        # Create a new diretory because it does not exist
        os.makedirs("./core")

    with open(f"core/{core_table}.sql", "w") as file:
        file.write(resp_core["data"]["dbt_sql"])
    file.close()

    with open(f"core/{core_table}.yml", "w") as file:
        file.write(resp_core["data"]["model"])
    file.close()

# %%
