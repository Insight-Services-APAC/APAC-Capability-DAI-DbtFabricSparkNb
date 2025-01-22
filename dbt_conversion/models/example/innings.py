from pyspark.dbutils import DBUtils

def model(dbt, session):
    dbt.config(materialized="table")

    sql = '''
        SELECT
		Team
		, innings_id
		, Over as MatchOver
		, delivery_id
		, concat(cast(Over as varchar(10)),'.',cast(delivery_id as varchar(10))) as Delivery
		, bowler
		, batter
		, non_striker
		, batter_runs
		, case when extras_byes is not null then extras_byes else 0 end as extras_byes
		, case when extras_wides is not null then extras_wides else 0 end as extras_wides
		, case when extras_legbyes is not null then extras_legbyes else 0 end as extras_legbyes
		, filenumber
	FROM 
		main.landing_cricketstats.tbl_innings'''
    
    df = spark.sql(sql)

    return df