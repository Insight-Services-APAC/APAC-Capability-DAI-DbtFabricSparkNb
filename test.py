import duckdb
from sqlframe import activate
connection = duckdb.connect("file.duckdb")
activate("duckdb", conn=connection)
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


sql = """
select 1 from test
"""

spark.sql(sql).show()


