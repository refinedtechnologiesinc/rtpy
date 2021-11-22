# %% imports
import logging
from azure.storage.blob import BlobServiceClient
import traceback2 as traceback
import pandas as pd

# following packages are from a Splintered Glass custom pacakge
# this can be installed from:
#   git+https://github.com/Splintered-Glass-Solutions/sgs@v0.3.17

import sgs.db.snowflake as sf
from sgs.util.auth import _getvar
import sgs.util.sql as sgql

# %% Azure creds from envi vars
az_conn = _getvar("AZURE_CONNECTION_STRING")['data']

# %% connect to blob storage
blob_service_client = BlobServiceClient.from_connection_string(az_conn)
# List containers in the storage account
# list_containers = list(blob_service_client.list_containers())
# print([l['name'] for l in list_containers])

# %% get list of files in certain fodler
# Instantiate a new ContainerClient
container = "ctm-exports"
container_client = blob_service_client.get_container_client(container)

# list files
list_blobs = list(container_client.list_blobs())
# %% find most recent
blobs = [l['name'] for l in list_blobs]
blobs.sort()
latest_name = blobs[0]
latest = [l for l in list_blobs if l['name'] == latest_name][0]

logging.info(f"Pulling file {latest['name']}.")


# %% download it get data from select sheet
blob_client = blob_service_client.get_blob_client(
    container=container, blob=latest['name'])

transactions_df = pd.read_excel(
    blob_client.download_blob().readall(), 'Transaction Detail', skipfooter=1)

# %% parse it into appropriate formatting
rename = {}
for col in list(transactions_df):
    rename[col] = sgql.column_name_clean(col)

transactions_df.rename(columns=rename, inplace=True)

# %% save to snowflake
warehouse = "LOADING"
db = "RAW"
schema = "ctm"
table = "transactions"
# %% Open connection
resp_c = sf.connect(warehouse, db, schema)
if resp_c['status'] == 200:
    conn = resp_c['data']

# %% upsert the data
sql_ui = sgql.df_upsert(df=transactions_df, table=table,
                        pk='invoice_number', conn=conn)

# %%
if resp_c['status'] == 200:
    logging.info(f"Upserted {len(transactions_df)} transactions.")
else:
    logging.error(resp_c['message'])
