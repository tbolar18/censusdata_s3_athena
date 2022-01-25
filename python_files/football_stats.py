import requests
import pandas as pd
from bs4 import BeautifulSoup
import s3fs
from collections import OrderedDict

s3 = s3fs.S3FileSystem(anon=False)

list_years = [2016,2017,2018,2019,2020]
all_years = []
for year in list_years:
    url = "https://www.nfl.com/standings/league/%d/REG" % year
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    for sup in soup.select('sup'):
        sup.extract()

    current_df = pd.read_html(str(soup))[0]
    current_df['Year'] = year
    all_years.append(current_df)

all_dfs = pd.concat(all_years)
all_dfs['NFL Team'] = (all_dfs['NFL Team'].str.split()
                      .apply(lambda x: OrderedDict.fromkeys(x).keys())
                      .str.join(' '))

with s3.open('census-stats-tejas-b/football_stats.csv','w') as f:
    all_dfs.to_csv(f,header = True, index=False, line_terminator='\n')