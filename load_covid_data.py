#!/usr/bin/python3.6

#script to pull covid data to sandbox

import requests
import pandas as pd
import io
import pyodbc
import sqlalchemy as sal
from sqlalchemy.types import Integer, String
import jmm_library as jl

config_file = 'jmm_creds.ini'
cfg = jl.load_config(config_file)
db_creds = dict(cfg.items('tower_db'))

url = 'http://covidtracking.com/api/states/daily.csv'
s = requests.get(url).content
df = pd.read_csv(io.StringIO(s.decode('utf-8')))
df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
df['state'] = df['state'].apply('str')

engine = sal.create_engine(f"mssql+pyodbc://{db_creds.get('user')}:{db_creds.get('pw')}@{db_creds.get('server')}:1433/{db_creds.get('db')}?driver={db_creds.get('driver_sal'}")

with engine.begin() as connection:
	df.to_sql(
						'jmm_covid_data'
						,connection
						,if_exists='replace'
						,index=False
						,dtype={
										'state':String()
										,'positive':Integer()
										,'negative':Integer()
										,'pending':Integer()
										,'hospitalizedCurrently':Integer()
										,'hospitalizedCumulative':Integer()
										,'inIcuCurrently':Integer()
										,'inIcuCumulative':Integer()
										,'onVentilatorCurrently':Integer()
										,'onVentilatorCumulative':Integer()
										,'recovered':Integer()
										,'dataQualityGrade':String()
										,'positiveIncrease':Integer()
										,'negativeIncrease':Integer()
										,'total':Integer()
										,'totalTestResults':Integer()
										,'deathIncrease':Integer()
										,'hospitalizedIncrease':Integer()
										}
								)

