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
schema = "itrack"

# ========== snowflake cleanup ============

# %% Open connection
resp_c = sf.connect(warehouse, db, schema)
if resp_c['status'] == 200:
    conn = resp_c['data']

# %% inputs
table_name = "invoice"
