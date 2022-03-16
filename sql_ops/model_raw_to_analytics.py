# %% take raw table and assuming no name changes, make the stg tbl and dbt code
import snowflake.connector
from sgs.util.auth import _getvar
import pprint as pp
import re
import clipboard
import logging
import sgs.db.snowflake as sf
from sgs.util.convert import _response
from sgs.util.sql import create_table_sql

# %% inputs
schema = "iview_projects_stead"
raw_table = "system"
fct_dim = "dim"
# deived
stg_table = f"stg_{schema}__{raw_table}"
core_table = f"{fct_dim}_{raw_table}"


# %% Open connection
resp_c = sf.connect("LOADING", "RAW", schema)
if resp_c['status'] == 200:
    conn_raw = resp_c['data']

# %% get the columns from the RAW table
resp_c = sf.get_columns(conn_raw, raw_table, sort=True)
if resp_c['status'] != 200:
    raise ValueError(resp_c)

cols = resp_c['data']
# add the id columns that dbt will use as the unique key
cols.append({'Name': "id", "Type": "VARCHAR(40)",
            "Unique": "Y", "NotNull": "Y"})

# %% do some auto cleaning IF NEEDED
for c in cols:
    v = c['Name']
    # set to lower
    v = v.lower()
    # replace space with _
    v = v.replace(" ", "_")
    # remove non-alphanumeric
    v = re.sub(r'[^a-zA-Z0-9_]', '', v)
    # update the rename_map
    c['As'] = v

# pp.pprint(rename_map)
# clipboard.copy(str(rename_map))

# [MANUAL STEP] do any additional renames IF NEEDED
# rename_map = {'INVOICE_NUMBER': 'invoice_number', 'PNR': 'pnr', 'TICKET_NUMBER': 'ticket_number', '_FIVETRAN_BATCH': '_fivetran_batch', 'TRAVELER_NAME': 'traveler_name', 'CARD_NUMBER': 'card_number', 'ORIGIN': 'origin', 'VENDOR_NAME': 'vendor_name', 'ITINERARY': 'itinerary', 'TYPE': 'type', 'DEST': 'destination', 'CARD_TYPE': 'card_type', 'PID_NUMBER': 'pid_number',
#               'INVOICE_DATE': 'invoice_date', 'CLIENTS_NAME': 'clients_name', 'FARE_CLASS_NAME': 'fare_class_name', '_FIVETRAN_INDEX': '_fivetran_index', 'TOTAL_AMOUNT': 'total_amount', 'BOOKING_SOURCE': 'booking_source', 'TRAVEL_ARRANGER': 'travel_arranger', 'SERVICE_DATE': 'service_date', 'RETURN_DATE': 'return_date', '_FIVETRAN_SYNCED': '_fivetran_synced'}

# If Existing Table in STG:  rename the snowflake columns
# for k, v in rename_map.items():
#     try:
#         cur = conn.cursor()
#         sql = f'alter table {schema}.stg_{schema}__{table} rename column "{k}" to {v}'
#         cur.execute(sql)
#         cur.close()
#     except Exception as e:
#         logging.warn(e)

#  Else Create the STG table sql
# resp_cs = create_table_sql(stg_table, cols)

# if resp_cs['status'] != 200:
#     raise ValueError(resp_cs)

# c_sql = resp_cs['data']
# clipboard.copy(c_sql)

# run the create stg tbl sql in SNOWFLAKE


# %% ========== dbt ============
# %% =============== define the basic dbt transform to take from raw and insert into stg
stg_dbt = f"""with source as (

    select * from {{{{ source('{schema}','{raw_table}') }}}}

),

staging as (

    select
        sha1(array_to_string(array_construct_compact(*),','))       as id,
        """

for c in cols:
    # skip id col since that gets defined as the sha1 above
    if c.get('Name') == 'id':
        continue
    stg_dbt += c.get('Name')
    spaces = 60 - len(c.get('As', c['Name']))
    stg_dbt += " " * spaces
    stg_dbt += f"""as {c.get('As', c['Name']).lower()},
        """
# remove the trailing comma
stg_dbt = stg_dbt[:-10]

stg_dbt += """
    from source

)

select * from staging"""

clipboard.copy(stg_dbt)


# %% create the schema.yml for this model
yml = f"- name: {stg_table}\n    description:  This table contains a staged version of '____' from ___.\n    columns:\n"
for c in cols:
    yml += f"      - name: {c.get('Name').lower()}\n        description: _____\n        tests:\n"
    if c.get('NotNull') == "Y" or c.get('PrimaryKey') == "Y":
        yml += "          - not_null\n"
    if c.get('UniqueKey') == "Y":
        yml += "          - unique\n"
    yml += "   \n"
clipboard.copy(yml)

# %% PASTE THAT INTO THE dbt repo the make an needed modification

# Open ANALYTICS connection
# resp_ca = sf.connect(warehouse="TRANSFORMING",
#                      db="ANALYTICS",
#                      schema="STAGING",
#                      role="TRANSFORMER")
# if resp_ca['status'] == 200:
#     conn_a = resp_ca['data']

#  get the columns from the STG table
# resp_c = sf.get_columns(conn_a, stg_table, sort=True)
# if resp_c['status'] != 200:
#     raise ValueError(resp_c)

# cols = resp_c['data']
#   Create the CORE table sql
# resp_cs = create_table_sql(core_table, cols)

# if resp_cs['status'] != 200:
#     raise ValueError(resp_cs)

# c_sql = resp_cs['data']
# clipboard.copy(c_sql)

#  RUN THIS CREATE SQL IN CORE

# %% =============== make the dbt core model sql
core_dbt = f"""with staging as (

    select * from {{{{ ref('{stg_table}') }}}}

),

final as (

    select
        """

# %% add to sql
for c in cols:
    core_dbt += f"""{c.get('As', c['Name'])},
        """

# remove the trailing comma
core_dbt = core_dbt[:-10]
core_dbt += """
    from staging
 
)

select * from final"""

clipboard.copy(core_dbt)

# %%
# %% create the schema.yml for this model
yml = f"- name: {core_table}\n    description:  This table contains a core version of '____' from ___.\n    columns:\n"
for c in cols:
    # if c.get('PrimaryKey') != "Y":
    #     continue
    yml += f"      - name: {c.get('Name').lower()}\n        description: _____\n        tests:\n"
    if c.get('NotNull') == "Y" or c.get('PrimaryKey') == "Y":
        yml += "          - not_null\n"
    if c.get('UniqueKey') == "Y":
        yml += "          - unique\n"
    yml += "   \n"
clipboard.copy(yml)
# %%
