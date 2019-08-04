import pandas as pd
import Functions as fct
import os

os.chdir("dump")

mapfile = pd.read_excel("BBGID_ISIN_RPID_final.xlsx")
universe = list(mapfile["RP_ENTITY_ID"])

print(len(universe))


end_date="2019-01-01 00:00:00"
start_date="2015-01-01 00:00:00"

fct.get_counts_universe(universe,"lt",start_date,end_date,90,"universe_neg")
fct.get_counts_universe(universe,"gte",start_date,end_date,90,"universe_pos")

