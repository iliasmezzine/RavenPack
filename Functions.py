from ravenpackapi import RPApi
from ravenpackapi import Dataset
import pandas as pd

apikey = "xYJLw50JQN1f4f6urKycmd"
api = RPApi(api_key=apikey)

#Extracts data (positive news count) from one entity

def get_counts(entity_id,ltgt,start_date,end_date,filename):

    label = "count_pos"
    if ltgt == "lt":
        label = "count_neg"
    global api

    custom_dataset = Dataset(
        name="Test set",
        frequency="daily",
        filters={"and": [
            {"rp_entity_id": entity_id},
            {"event_relevance": { "gte": 90 }},
            {"event_sentiment_score":{ ltgt: 0.5 }},
        ]},
        custom_fields=[
            {label: {
                "count": {
                    "field": "rp_entity_id"
                }
            }
            },

        ]
    )
    ds = api.create_dataset(custom_dataset)
    req_data = ds.request_datafile(start_date=start_date,end_date=end_date)
    fname = filename + ".csv"

    with open(fname,"w") as fp:
        req_data.save_to_file(filename=fp.name)
        fp.close()

    print("Done importing data for {}".format(fname))

#Extracts data from a universe (list of RP_ENTITY_ID == universe parameter)

def get_counts_universe(universe,ltgt,start_date,end_date,thresh,filename):

    label = "count_pos"

    if ltgt == "lt":
        label = "count_neg"

    global api
    custom_dataset = Dataset(
        name="Test set",
        frequency="daily",
        filters={"and": [
            {"rp_entity_id": {"in": universe}},
            {"event_relevance": { "gte": thresh }},
            {"event_sentiment_score":{ ltgt: 0.5 }},
        ]},
        custom_fields=[
            {label: {
                "count": {
                    "field": "rp_entity_id"
                }
            }
            },

        ]
    )
    ds = api.create_dataset(custom_dataset)
    req_data = ds.request_datafile(start_date=start_date,end_date=end_date)
    fname = filename+".csv"

    with open(fname,"w") as fp:
        req_data.save_to_file(filename=fp.name)
        fp.close()

    print("Done importing data for {}".format(fname))

#Cleans name in BBG_ID

def tf_name(x):
    return x.replace("/","#")

def inv_name(x):
    return x.replace("#","/")

def ind_func(x):
    if x > 0 :
        return 1
    elif x <0 :
        return -1
    else:
        return 0
def clean_eqty_name(x):
    return x.replace("MERGED_","").replace(".xlsx","")

def product(arr):
    pr = 1
    for u in arr:
        pr = pr*u
    return pr

def clean_df(df,replace_name):
    return df.rename(columns={df.columns[0]: clean_eqty_name(replace_name)})

