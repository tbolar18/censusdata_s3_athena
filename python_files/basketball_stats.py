import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np

import s3fs

s3 = s3fs.S3FileSystem(anon=False)

pd.set_option('display.max_columns', None)
list_years = [2016,2017,2018,2019,2020]
index = 0
all_years = []
for years in list_years:
    year = list_years[index]
    current_df = pd.read_html("https://www.basketball-reference.com/leagues/NBA_%d.html" % year)
    current_df_east = current_df[0]
    current_df_east['Year'] = year - 1
    current_df_west = current_df[1]
    current_df_west['Year'] = year - 1

    all_years.append(current_df_east)
    all_years.append(current_df_west)
    index = index + 1
all_dfs = pd.concat(all_years)

all_dfs.columns = ['TeamE', 'W', 'L', 'W/L%', 'GB', 'PS/G', 'PA/G', 'SRS','Year','TeamW']

#all_dfs["Team"] = all_dfs['TeamE'].astype(str) + all_dfs['TeamW'].astype(str)
all_dfs["Team"] = np.where(all_dfs['TeamW'].isnull(),all_dfs['TeamE'],all_dfs['TeamW'])
all_dfs = all_dfs.drop(columns=['TeamE','TeamW'])
all_dfs['Team'] =  all_dfs['Team'].replace('[^a-zA-Z ]', '', regex=True)
all_dfs['Team'] =  all_dfs['Team'].replace(['Philadelphia ers'],'Philadelphia 76ers')
print(all_dfs.columns)


with s3.open('census-stats-tejas-b/basketball_stats.csv','w') as f:
    all_dfs.to_csv(f,header = True, index=False, line_terminator='\n')

