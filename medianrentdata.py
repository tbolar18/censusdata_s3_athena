import requests
import pandas as pd
import s3fs

s3 = s3fs.S3FileSystem(anon=False)

item = 'B25064_001E'
year_list = ['2015','2016','2017','2018','2019']
dataset_list= []
for year in year_list:
        r= requests.get("https://api.census.gov/data/%s/acs/acs1?get=NAME,%s&for=county:*&in=state:*&key=ea438bad26dbab19a2b95f6ed8c73a7b1c8543d1" %(year,item))
        df = pd.DataFrame(r.json())
        df=df[1:]
        df[4] = item
        df[5] = year
        dataset_list.append(df)
dataset = pd.concat(dataset_list)
dataset.columns = ['name','median_gross_rent','state_id','county_id','item','year']

split_name = dataset['name'].str.split(",", n=1, expand=True)
dataset["county"] = split_name[0]
dataset["state"] = split_name[1]

dataset.drop(columns= ["name","item"], inplace = True)
dataset.columns =['median_gross_rent','state_id','county_id','year','county','state']
with s3.open('census-stats-tejas-b/medianrentdata.csv','w') as f:
    dataset.to_csv(f,header = True, index=False, line_terminator='\n')