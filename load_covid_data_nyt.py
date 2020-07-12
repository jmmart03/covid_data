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

#get state data
url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv'
s = requests.get(url).content
df = pd.read_csv(io.StringIO(s.decode('utf-8')))
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
df['state'] = df['state'].apply('str')
df['fips'] = df['fips'].apply(str)

#get county data
county_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv'
sc = requests.get(county_url).content
col_names = pd.read_csv(io.StringIO(sc.decode('utf-8')),nrows=0).columns
types_dict = {'cases':int, 'deaths':int}
types_dict.update({col: str for col in col_names if col not in types_dict})
df_c = pd.read_csv(io.StringIO(sc.decode('utf-8')),dtype=types_dict)
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

engine = sal.create_engine(f"mssql+pyodbc://{db_creds.get('user')}:{db_creds.get('pw')}@{db_creds.get('server')}:1433/{db_creds.get('db')}?driver={db_creds.get('driver_sal'}")

with engine.begin() as connection:
	df.to_sql(
		'jmm_covid_data_nyt_state'
		,connection
		,if_exists='replace'
		,index=False
		,dtype={
			'state':String()
			,'fips':String()
			}
		)
	df_c.to_sql(
		,'jmm_covid_data_nyt_county'
		,connection
		,if_exists='replace'
		,index=False
		,dtype={
			'state':String()
			,'fips':String()
			,'county':String()
			}
		)

