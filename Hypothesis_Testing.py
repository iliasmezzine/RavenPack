import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import ttest_ind as tt
import Functions as fct
from itertools import groupby
import os

#HYP 1 : DIF changes impact forward returns.
# Windows : 90 days, 45 days, 30 days, 7 days
# Perform t-Test with hypothesis = Spread == 0

os.chdir("dump/dump_test/merged")
files = os.listdir()

spreads_90_df = []
spreads_45_df = []
spreads_30_df = []
spreads_7_df = []

save_dict = {"IND_DIF_90d":spreads_90_df,
             "IND_DIF_45d":spreads_45_df,
             "IND_DIF_30d":spreads_30_df,
             "IND_DIF_7d":spreads_7_df}
removed_list = []

print(files)

excl_list = ['~$MERGED_1COV GY Equity.xlsx', '~$MERGED_ZURN SW Equity.xlsx']

for file in files[2:] :
    if file not in excl_list:

        df = pd.read_excel(file).dropna(axis=0)
        x = list(df["OPEN"])
        grps = [list(j) for i, j in groupby(x)]
        max_iter_prop  = np.max([len(u) / len(x) for u in grps])

        # Filter files with too much missing data (abnormal level of equal returns correspond to removed stocks)
        # Threshold : more than 15% redundant values in df["OPEN"]

        if max_iter_prop < 0.15:

            df.set_index(df.columns[0],inplace=True)
            df = df[["O2C_RETURN_" + str(i) for i in range(7)] + ["IND_DIF_90d","IND_DIF_45d","IND_DIF_30d","IND_DIF_7d"]]

            for key in save_dict.keys():

                gp_ind = df.groupby(key).agg("mean")
                spreads = gp_ind.loc[1] - gp_ind.loc[-1]
                spreads_to_df = pd.DataFrame(spreads).rename(columns={0:fct.clean_eqty_name(file)})
                save_dict[key].append(spreads_to_df)
                print("APPENDED : " + file + " " + key )

        else:

            print("REMOVED : " + file)
            removed_list.append(file)

print(save_dict["IND_DIF_90d"])

merged = [[u , pd.concat(save_dict[u],axis=1).transpose()] for u in save_dict.keys()]

for x,y in merged:
    y.to_excel("AVG_SPREADS - "+ x + ".xlsx")

pd.DataFrame(removed_list).to_excel("REMOVED.xlsx")
