# %% imports
from azure.storage.blob import BlobServiceClient
import logging
import os
import pandas as pd

# following packages are from a Splintered Glass custom pacakge
# this can be installed from:
#   git+https://github.com/Splintered-Glass-Solutions/sgs@v0.3.17

import sgs.db.snowflake as sf
from sgs.util.convert import _response
from sgs.util.sql import df_to_create_sql, column_name_clean
from sgs.util.auth import _getvar

# %% Azure creds from envi vars
az_conn = _getvar("AZURE_CONNECTION_STRING")['data']

# %% connect to blob storage
blob_service_client = BlobServiceClient.from_connection_string(az_conn)
# List containers in the storage account
list_containers = list(blob_service_client.list_containers())
print([l['name'] for l in list_containers])

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

# %% download it get data from select sheet
blob_client = blob_service_client.get_blob_client(
    container=container, blob=latest['name'])

transactions_df = pd.read_excel(
    blob_client.download_blob().readall(), 'Transaction Detail', skipfooter=1)

# %% parse it into appropriate formatting
rename = {}
for col in list(transactions_df):
    rename[col] = column_name_clean(col)

transactions_df.rename(columns=rename, inplace=True)

# %% save to snowflake
warehouse = "LOADING"
db = "RAW"
schema = "ctm"
table = "transactions"

# %% ========== create table if needed (one time) ============
resp_sc = df_to_create_sql(transactions_df, table)
if resp_sc['status'] != 200:
    print(resp_sc)
sql_create = resp_sc['data']


# %% TODO: move these to sgs.util.sql
def row_to_insert_sql(row, table):
    row_vals = [str(val) for val in row.values]
    return ('INSERT INTO ' + table + ' (' +
            str(', '.join(list(row.keys()))) + ') VALUES ' + str(tuple(row_vals)))


def row_to_update_sql(row, table, pk=None):
    sql = 'UPDATE ' + table + ' SET'
    for i, col in enumerate(list(row.keys())):
        sql += f" {col} = '{str(row[col])}'"
        if i < len(list(row.keys())) - 1:
            sql += ","
    if pk:
        sql += f" WHERE {pk} = '{row[pk]}'"
    sql += ";"
    return sql


def df_to_insert_sql(df, table):
    sql_texts = []
    for index, row in df.iterrows():
        sql_texts.append(row_to_insert_sql(row, table))
    return sql_texts


def df_to_update_sql(df, table, pk):
    sql_texts = []
    for index, row in df.iterrows():
        sql_texts.append(row_to_update_sql(row, table, pk))
    return sql_texts


def df_to_bulkinsert_sql(df, table):
    sql = 'INSERT INTO ' + table + \
        ' (' + str(', '.join(df.columns)) + ') VALUES'
    for index, row in df.iterrows():
        sql += " " + str(tuple(row.values))
    sql += ";"
    return sql

# TODO: write a upsert function
# check if pk exists, if so update else insert


# %%
sql_i = df_to_insert_sql(transactions_df, table)
sql_u = df_to_update_sql(transactions_df, table, 'invoice_number')

# %%


# %% Open connection
resp_c = sf.connect(warehouse, db, schema)
if resp_c['status'] == 200:
    conn = resp_c['data']

# %%
cur = conn.cursor()
cur.execute(sql_u[1])
cur.close()

# %%
