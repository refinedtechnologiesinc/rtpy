# %% connect to snowflake cluster
import pprint as pp
import re
import clipboard
import logging
import sgs.db.snowflake as sf
from sgs.util.convert import _response

# %% inputs
warehouse = "LOADING"
db = "RAW"
schema = "sage"
table = "coa"

# ========== snowflake cleanup ============

# %% Open connection
resp_c = sf.connect(warehouse, db, schema)
if resp_c['status'] == 200:
    conn = resp_c['data']

# %% get the columns
cols = sf.get_columns(conn, schema, table)

# %% do some auto cleaning
rename_map = dict(zip(cols, cols))

for k, v in rename_map.items():
    # set to lower
    v = v.lower()
    # replace space with _
    v = v.replace(" ", "_")
    # remove non-alphanumeric
    v = re.sub(r'[^a-zA-Z0-9_]', '', v)
    # update the rename_map
    rename_map[k] = v

pp.pprint(rename_map)
clipboard.copy(str(rename_map))

# %% [MANUAL STEP] do any additional renames needed
rename_map = {'account_id': 'account_id', 'account_description': 'account_description', 'is_active': 'is_active', 'account_type': 'account_type', 'financial_category': 'financial_category', 'account_type2': 'account_type2', 'segment': 'segment', 'region': 'region', 'entity': 'entity',
              'source': 'source', 'sub_account_type': 'sub_account_type', 'equipment_type': 'equipment_type', 'sub_account_type_description': 'sub_account_type_description', 'owner': 'owner', 'department': 'department', 'mr_type': 'mr_type', 'purpose': 'purpose', 'examples': 'examples'}
# %% Rename the snowflake columns
for k, v in rename_map.items():
    try:
        cur = conn.cursor()
        sql = f'alter table {schema}.{table} rename column "{k}" to {v}'
        cur.execute(sql)
        cur.close()
    except Exception as e:
        logging.warn(e)

# ========== dbt ============
# %% make the dbt staging model
stg_dbt = f"""with source as (

    select * from {{{{ source('{schema}','{table}') }}}}

),

staging as (

    select
        sha1(array_to_string(array_construct_compact(*),','))       as id,
        """

# %% get the columns
cols = util.get_columns(conn, schema, table)

# %% add to sql
for c in cols:
    stg_dbt += c
    spaces = 60 - len(c)
    stg_dbt += " " * spaces
    stg_dbt += f"""as {c},
        """
stg_dbt += """
    from source

)

select * from staging"""

# %%
clipboard.copy(stg_dbt)

# %% make the dbt core model
core_dbt = f"""with staging as (

    select * from {{{{ ref('stg_{schema}__{table}') }}}}

),

final as (

    select
        """

# %% get the columns
cols = util.get_columns(conn, schema, table)

# %% add to sql
for c in cols:
    core_dbt += f"""{c},
        """
core_dbt += """
    from staging
 
)

select * from final"""

# %%
clipboard.copy(core_dbt)

# %%
