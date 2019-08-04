import pandas as pd
import os

os.chdir("dump\dump_test")
file = "AMEAS FH Equity.xlsx"
df = pd.read_excel(file)
print(df.dropna(axis=0))

#Splits SXXP file into single stock files and renaming columns

df = pd.read_excel("SXXP.xlsx").set_index("Unnamed: 0")
print("Done importing file !")
nb_col = len(df.columns)
split_dfs= []
except_count = 0

for i in range(nb_col):
    try:
        u = df.iloc[:,3*i:3*(i+1)]
        split_dfs.append(u)
        filename = "df_split_"+str(i)+".xlsx"
        u.to_excel(filename)
        print("Dumping file : "+ filename)
    except:
        except_count+=1
        pass

print("Done !")
print("Nb of exceptions : " + str(except_count))

