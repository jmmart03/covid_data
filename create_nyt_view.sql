--script to create covid data views based on NYT data

create view dbo.jmm_covid_data_nyt_state_view as 

with x as (
  select
    s.*
		,s.cases-isnull(s1.cases,0) as 'new_cases'
		,s.deaths-isnull(s1.deaths,0) as 'new_deaths'
	from tower..jmm_covid_data_nyt_state s
		left join tower..jmm_covid_data_nyt_state s1 on s1.date=dateadd(day,-1,s.date)
			and s1.state=s.state
	)
select
	x.date
	,x.state
	,x.fips
	,x.cases
	,x.deaths
	,x.new_cases
	,x.new_deaths
	,avg(x1.new_cases) as 'new_cases_7dma'
	,avg(x1.new_deaths) as 'new_deaths_7dma'
from x
	left join x x1 on x1.state=x.state
		and x1.date>dateadd(day,-7,x.date)
		and x1.date<=x.date
group by
	x.date
	,x.state
	,x.fips
	,x.cases
	,x.deaths
	,x.new_cases
	,x.new_deaths
