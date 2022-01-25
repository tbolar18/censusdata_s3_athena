import requests
import pandas as pd
from pandas import json_normalize
import s3fs
import json

import s3fs

s3 = s3fs.S3FileSystem(anon=False)

def subject_subscale(subject):
    switcher = {
        'mathematics' : 'MRPCM',
        'reading' : 'RRPCM',
        'writing' : 'WRIRP',
    }
    return switcher.get(subject,"nothing")

jurisdictions = ['AL','AK','AZ','AR'
                ,'CA','CO','CT','DE',
                'DC','DS','FL','GA','HI','ID','IL','IN',
                'IA','KS','KY','LA','ME','MD','MA','MI',
                'MN','MS','MO','MT','NE','NV','NH','NJ',
                'NM','NY','NC','ND','OH','OK','OR','PA',
                'RI','SC','SD','TN','TX','UT','VT','VA',
                'WA','WV','WI','WY']
year_list = ['2015','2016','2017','2018','2019']
grade_list = ['4','8','12']
subject_list = ['mathematics','reading','writing']
dataset_list = []

for jurisdiction in jurisdictions:
    for year in year_list:
        for grade in grade_list:
            for subject in subject_list:
                subscale = subject_subscale(subject)
                r = requests.get(
                    "https://www.nationsreportcard.gov/Dataservice/GetAdhocData.aspx?type=data&subject=%s&grade=%s&subscale=%s&variable=TOTAL&jurisdiction=%s&stattype=MN:MN&Year=%s"
                    % (subject,grade,subscale,jurisdiction,year))
                rtext = r.text
                if rtext.startswith('{"status":200'):
                    data = r.json()
                    df = pd.DataFrame.from_dict(data)
                    new_df = pd.json_normalize(df['result'])
                    dataset_list.append(new_df)


dataset = pd.concat(dataset_list)
dataset = dataset.drop(columns=['sample','yearSampleLabel','Cohort',
                                'CohortLabel','stattype','scale',
                                'variable','variableLabel','varValue',
                                'varValueLabel','isStatDisplayable','errorFlag'])

with s3.open('census-stats-tejas-b/education_stats/education_stats.csv','w') as f:
    dataset.to_csv(f,header = True, index=False, line_terminator='\n')