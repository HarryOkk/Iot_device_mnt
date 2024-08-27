import pandas as pd

df_src = pd.read_csv("under_cloud_zzggll20240808-1723092527639.csv",low_memory=False)
df_sroted = df_src.sort_values(by=['DEVICE_ID', 'UPLOADTIME'], ascending=[True,False])
df_latest = df_sroted.drop_duplicates(subset='DEVICE_ID', keep='first')

df_cloud = pd.read_csv("cloud_zzggll20240808.csv",low_memory=False)


# 去掉df_latest中和df_cloud重复device_id的行
df_latest = df_latest[~df_latest['DEVICE_ID'].isin(df_cloud['DEVICE_ID'])]
# 只保留部分列
df_latest = df_latest[['DEVICE_ID', 'UPLOADTIME']]
df_latest.describe()