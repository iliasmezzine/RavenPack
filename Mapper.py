import pandas as pd
from ravenpackapi import RPApi
import os

apikey = "xYJLw50JQN1f4f6urKycmd"
api = RPApi(api_key=apikey)

#Map one entity
def get_rpid(stock):
    global api
    try:
        return [m.id for m in api.get_entity_mapping([stock]).matched][0]
    except:
        return "NaN"

#Map everything

ds = pd.read_excel("Isin.xlsx")
bbgid = list(ds["BBG Ticker"])
isins = list(ds["ID_ISIN"])

mapped = []
for x,y in list(zip(bbgid,isins)):
    s = get_rpid(y)
    mapped.append([x,y,s])
    print([x,y,s])

df = pd.DataFrame(mapped)
os.chdir("dump")
df.to_excel("BBGID_ISIN_RPID.xlsx")
print(df)

