import pandas as pd
import os
import numpy as np
import Functions as fct

os.chdir("dump")

## Dict BBG_ID -> RP_ENTITY_ID

mapfile = pd.read_excel("BBGID_ISIN_RPID_final.xlsx")
universe_rpid = list(mapfile["RP_ENTITY_ID"])
universe_bbgid = list(mapfile["BBG_ID"])
entity_dict = dict(zip(list(mapfile["BBG_ID"]),list(mapfile["RP_ENTITY_ID"])))

# Creates pos / neg sentiment df for given universe

ds_neg = pd.read_csv("universe_neg.csv")
ds_pos = pd.read_csv("universe_pos.csv")

grouped_pos = ds_pos.groupby("RP_ENTITY_ID")
grouped_neg = ds_neg.groupby("RP_ENTITY_ID")

pos_sent_dfs = [y for x,y in grouped_pos]
neg_sent_dfs = [y for x,y in grouped_neg]

cct  = [pd.concat([u.set_index("TIMESTAMP_UTC"),v.set_index("TIMESTAMP_UTC")],axis=1) for u,v in list(zip(pos_sent_dfs,neg_sent_dfs))]

#Create DIF 90d indicator in each df

for df in cct:

    df["DIFF"] = df["COUNT_POS"] - df["COUNT_NEG"]

    df["DIF_90d"] = df["DIFF"].rolling(90).sum()
    df["DIF_45d"] = df["DIFF"].rolling(45).sum()
    df["DIF_30d"] = df["DIFF"].rolling(30).sum()
    df["DIF_7d"] = df["DIFF"].rolling(7).sum()

    df["IND_DIF_90d"] = df["DIF_90d"].diff().apply(fct.ind_func)
    df["IND_DIF_45d"] = df["DIF_45d"].diff().apply(fct.ind_func)
    df["IND_DIF_30d"] = df["DIF_30d"].diff().apply(fct.ind_func)
    df["IND_DIF_7d"] = df["DIF_7d"].diff().apply(fct.ind_func)

#Defining Dict mapping RP_ENTITY_ID with corresponding pos/neg dataframe

mapped_cct = {z["RP_ENTITY_ID"].iloc[0].iloc[0]:z for z in cct}
os.chdir("dump_test")

#Merging and Dumping each merged Stock / Sentiment file

for bbgid in universe_bbgid[:10]:

    ds_stock = pd.read_excel(fct.tf_name(bbgid) + ".xlsx")
    x = entity_dict[bbgid]

    if x in list(mapped_cct.keys()):


        ds_sent = mapped_cct[x].reset_index()
        col0 = ds_stock.columns[1]
        ds_stock[col0] = pd.to_datetime(ds_stock[col0]).dt.date
        ds_sent["TIMESTAMP_UTC"] = pd.to_datetime(ds_sent["TIMESTAMP_UTC"]).dt.date

        ds_stock = ds_stock.dropna(axis=0)
        ds_stock.set_index(col0, inplace=True)
        ds_stock.index.rename("TIMESTAMP_UTC", inplace=True)
        ds_sent.set_index("TIMESTAMP_UTC", inplace=True)

        merge = pd.concat([ds_stock, ds_sent], axis=1).dropna(axis=0)
        merge["O2C_RETURN_0"] = merge["CLOSE"] / merge["OPEN"] - 1


        for i in range(6):
            merge["O2C_RETURN_" + str((i + 1))] = merge["O2C_RETURN_0"].shift(-(i + 1))

        os.chdir("merged")
        merge.to_excel("MERGED_"+fct.tf_name(bbgid)+".xlsx")
        print("DONE : MERGED_"+fct.tf_name(bbgid))
        os.chdir("..")

