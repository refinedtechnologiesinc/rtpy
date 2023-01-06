# %%
import pandas as pd

# %%
df1 = pd.read_excel("../../Contract Status Report v2.xls")
# df1 = df1[df1["PID"].notnull()]

# %%
len(df1["Contract"].unique())
