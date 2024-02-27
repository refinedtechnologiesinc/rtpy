# write a script to connect to this data base and then read all the tables in the databse
# then print the listof tabels

# %%
import pyodbc
import pandas as pd

# Connection information
server = 'itrack.southcentralus.cloudapp.azure.com'
database = 'RTI'
username = 'fivetran'
password = 'm$Ju6!3$pUDFKMYR'
port = 1433

# Create a connection string
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};PORT={port}'

# Connect to the database
conn = pyodbc.connect(conn_str)

# Get a cursor
cursor = conn.cursor()

# Get the list of tables
tables = cursor.tables(tableType='TABLE')

# Print the list of tables
for table in tables:
  print(table.table_name)

# Close the cursor and connection
cursor.close()
conn.close()
