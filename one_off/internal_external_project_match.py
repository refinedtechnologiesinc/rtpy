# %% take raw table and assuming no name changes, make the stg tbl and dbt code
import traceback2 as traceback
import datetime as dt
import pandas as pd

from sgs.db.snowflake.connect import connect
from sgs.db.sql.write import df_upsert, df_to_bulkinsert_sql

from sgs.db.sql.utility import execute
from sgs.util.fuzzy import record_search
from sgs.util.log import _log

# %% the current table to model to a downstram dbt sql
db = "ANALYTICS"
raw_schema = "CORE"
warehouse = "TRANSFORMING"
role = "TRANSFORMER"
tbl = "market_oppertunities"

resp_conn = connect(
    warehouse=warehouse,
    db=db,
    schema=raw_schema,
    role=role,
    SF_USER="PPOPE",
    SF_PASSWORD="xeaHy8iyf64#",
)
if resp_conn["status"] != 200:
    raise resp_conn
conn_core = resp_conn["data"]

#%%
query = """Select NATIONAL_ACCOUNT, ACCOUNT_NAME, u.UNIT_NAME, PROJECT_ID, PROJECT_NAME, SALESFORCE_PROJECT_ID, START_DATE, TERRITORY
from salesforce_projects as p
left join salesforce_units as u
using(UNIT_ID);"""
resp_sfp = execute(conn=conn_core, statement=query, as_records=True)
if resp_sfp["status"] != 200:
    raise resp_sfp
sf_projects = resp_sfp["data"]

#%%
query = """Select PROJECT_ID, PROJECT_NAME, PLANT_CITY, 
PLANT_ADDRESS, PLANT_PARENT, TERRITORY, PLANT_COUNTRY,
PLANT_NAME,OWNER_NAME, START_DATE, CONCAT(PROJECT_NAME, ' - ', PLANT_PARENT) as TXT
from market_oppertunties as m;"""
resp_exp = execute(conn=conn_core, statement=query, as_records=True)
if resp_exp["status"] != 200:
    raise resp_exp
ex_projects = resp_exp["data"]


# %%
match_records = []

#%%
prj = [
    p
    for p in sf_projects
    if p["SALESFORCE_PROJECT_ID"] == "0061R000015T09AQAS"
][0]
#%%


for prj in sf_projects:
    _log(f"--Searching for match to pid: {prj['PROJECT_ID']}")
    # fzy match GLOBAL_ACCOUNT to parent name
    col_mapping = {}
    if prj.get("ACCOUNT_NAME") and prj.get("PROJECT_NAME"):
        prj["TXT"] = prj["ACCOUNT_NAME"] + " " + prj["PROJECT_NAME"]
        col_mapping["TXT"] = prj["TXT"]
    if prj.get("ACCOUNT_NAME"):
        col_mapping["PLANT_NAME"] = prj["ACCOUNT_NAME"][:-10]
        col_mapping["PLANT_CITY"] = prj["ACCOUNT_NAME"]
    if prj.get("NATIONAL_ACCOUNT"):
        col_mapping["PLANT_PARENT"] = prj["NATIONAL_ACCOUNT"][:5]
        col_mapping["OWNER_NAME"] = prj["NATIONAL_ACCOUNT"][:5]
    if prj.get("PROJECT_NAME"):
        col_mapping["PROJECT_NAME"] = prj["PROJECT_NAME"][:10]
    if prj.get("TERRITORY"):
        col_mapping["PLANT_COUNTRY"] = prj["TERRITORY"]

    if prj.get("START_DATE"):
        st = prj["START_DATE"] - dt.timedelta(days=365)
        end = prj["START_DATE"] + dt.timedelta(days=365)

        # limit down to has partial name match on owner and
        ex_projectsi = [
            ex
            for ex in ex_projects
            if ex["START_DATE"] > st
            and ex["START_DATE"] < end
            and prj["ACCOUNT_NAME"][:3].upper() in ex["PLANT_PARENT"].upper()
        ]
    else:
        ex_projectsi = ex_projects

    resp_mtchs = record_search(
        records=ex_projectsi, query=col_mapping, fuzzy=35, top=5
    )
    if resp_mtchs["status"] != 200:
        _log("None found")
        continue
    matches = resp_mtchs["data"]
    _log(len(matches))
    _log(matches[0]["ratio"])

    for match in matches:
        if match["ratio"] > 20:
            _log(prj["PROJECT_NAME"])
            _log(match["match"]["PROJECT_NAME"])
            match_rec = {}
            if not prj.get("SALESFORCE_PROJECT_ID") and not match.get(
                "PROJECT_ID"
            ):
                continue
            match_rec["internal_id"] = prj["SALESFORCE_PROJECT_ID"]
            match_rec["external_id"] = match["match"]["PROJECT_ID"]
            match_rec["match_id"] = (
                match_rec["internal_id"] + "-" + match_rec["external_id"]
            )
            match_rec["ratio"] = match["ratio"]
            match_records.append(match_rec)

# %%
_log(len(match_records))
df = pd.DataFrame(data=match_records)
# convert ratio to numperical
df["ratio"] = pd.to_numeric(df["ratio"])

# %% the generated table
db = "GENERATED"
schema = "PROJECT_PIPELINE"
warehouse = "TRANSFORMING"
role = "TRANSFORMER"
tbl = "PROJECT_MATCHES"

resp_conn = connect(
    warehouse=warehouse,
    db=db,
    schema=schema,
    role=role,
    SF_USER="PPOPE",
    SF_PASSWORD="xeaHy8iyf64#",
)
if resp_conn["status"] != 200:
    raise resp_conn
conn_gen = resp_conn["data"]

#%% clear out table
create = """create or replace table PROJECT_PIPELINE.PROJECT_MATCHES (
    internal_id varchar(255),
    external_id varchar(255),
    match_id varchar(255),
    ratio number
    );"""
resp_crt = execute(conn=conn_gen, statement=create, as_records=True)


#%% do a bulk insert
resp_sqli = df_to_bulkinsert_sql(df=df, table=tbl)
sql_i = resp_sqli["data"]


resp_i = execute(conn=conn_gen, statement=sql_i, as_records=True)

if resp_i["status"] != 200:
    raise resp_sqli


#%% upsert in batches on 1000
for i in range(150):
    _log(i)
    # resp_up = df_upsert(
    #     df=df.iloc[i * 1000 : 1000 * (i + 1)],
    #     conn=conn_gen,
    #     table=tbl,
    #     pk="match_id",
    #     trigger=True,
    # )
    dfi = df.iloc[i * 1000 : 1000 * (i + 1)]
    if not len(dfi):
        continue
    _log(len(dfi))
    resp_sqli = df_to_bulkinsert_sql(df=dfi, table=tbl)
    sql_i = resp_sqli["data"]

    resp_i = execute(conn=conn_gen, statement=sql_i, as_records=True)
#     _log(resp_up["message"])
# %%
# upsert_i = resp_up["data"][10]
# # upsert_i = "MERGE INTO PROJECT_MATCHES AS I USING( VALUES ('12414', '300500559', '12414-300500559', 36.0)) as s( internal_id, external_id, match_id, ratio) ON I.match_id = s.match_id WHEN MATCHED THEN UPDATE SET internal_id = '12414', external_id = '300500559', match_id = '12414-300500559', ratio = 36.0 WHEN NOT MATCHED THEN INSERT (internal_id, external_id, match_id, ratio) VALUES ('12414', '300500559', '12414-300500559', 36.0);"
# resp_e = execute(conn=conn_gen, statement=upsert_i, as_records=True)
# resp_e

# %%
