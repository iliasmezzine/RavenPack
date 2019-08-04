import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
#from scipy.stats import ttest_ind as tt
import Functions as fct
from itertools import groupby
import os

# #HYP 2 : ESS_DIF changes impact forward COMPOUND returns
# Calculate mean forward returns in NEGATIVE, NEUTRAL, POSITIVE ESS_DIF changes groups.
# Windows : 90 days, 45 days, 30 days, 7 days
# Perform t-Test with hypothesis = Spread == 0

os.chdir("dump/dump_test/merged")
files = os.listdir()

ind_names = ["IND_DIF_90d", "IND_DIF_45d", "IND_DIF_30d", "IND_DIF_7d"]
ind_dict = {ind:{"NEGATIVE":[],"NEUTRAL":[],"POSITIVE":[]} for ind in ind_names}
excl_list = ['~$MERGED_1COV GY Equity.xlsx', '~$MERGED_ZURN SW Equity.xlsx']
map_dict = {-1:"NEGATIVE",0:"NEUTRAL",1:"POSITIVE"}

for file in files[2:] :

    if file not in excl_list:

        df = pd.read_excel(file).dropna(axis=0)
        print(file)
        x = list(df["OPEN"])
        grps = [list(j) for i, j in groupby(x)]
        max_iter_prop  = np.max([len(u) / len(x) for u in grps])

        # Filter files with too much missing data (abnormal level of equal returns correspond to removed stocks)
        # Threshold : more than 15% redundant values in df["OPEN"]

        if max_iter_prop < 0.15:

            df.set_index(df.columns[0],inplace=True)

            #Defining compound forward returns
            fwd_returns = [1 + df["O2C_RETURN_" + str(i)] for i in range(7)]

            for i in range(2, 7):

                df["FWD_COMPOUND_" + str(i - 1)] = fct.product(fwd_returns[:i])

            # Keeping only forward returns and indicators for aggregation
            df = df[["FWD_COMPOUND_" + str(i-1) for i in range(2,7)] + ind_names]
                
            for ind in ind_names:

                # Keeping only single indicator in loop for aggregation
                gp_ind = df[["FWD_COMPOUND_" + str(i - 1) for i in range(2, 7)] + [ind]].groupby(ind).agg("mean")
                gplist = [fct.clean_df(pd.DataFrame(gp_ind.loc[i]), file) for i in range(-1, 2)]
                print(gp_ind)

                #Then assign each indicator in ind_dict to its position in the ind_dict dict.
                for key in map_dict.keys():
                    ind_dict[ind][map_dict[key]].append(gp_ind.loc[key])

#ind_dict = {ind:{"NEGATIVE":[],"NEUTRAL":[],"POSITIVE":[]} for ind in ind_names}

for ind_key in ind_dict.keys():
    for sign_key in ind_dict[ind_key].keys():
        conc = pd.concat(ind_dict[ind_key][sign_key],axis=1).transpose()
        conc.to_excel(ind_key + "-" + sign_key + ".xlsx")


