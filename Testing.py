# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Get Schemas from Schema Creation Notebook
dbutils.notebook.run("/Repos/aden.victor@qualyfi.co.uk/Star-Schema/SchemaCreation", timeout_seconds=1800)
# Get Queries from Queries Notebook
dbutils.notebook.run("/Repos/aden.victor@qualyfi.co.uk/Star-Schema/Queries", timeout_seconds=1800)

# Testing Schemas

assert trip_fact.schema == trip_fact_schema, "Schema mismatch for trip_fact table"
assert trip_date_dimension.schema == trip_date_dimension_schema, "Schema mismatch for trip_date_dimension table"

# Testing Queries

assert query_money_month.count() == 12, "Should only be 12 rows for the 12 months"

assert query_money_year.count() == 10, "Should only be 10 rows for years 2013-2022"

value = query_money_member.filter(col("age at joining") == 21).collect()
assert value[0][0] == 21, "Most money spent should be by members who were 21 when they joined"
assert value[0][1] == 495225.00, "Members at this age should have spent (currency)495,225.00"
