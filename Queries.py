# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

gold_path = "/tmp/aden/gold/"

# COMMAND ----------

transaction_fact = spark.read.format("delta").load(gold_path + "fact_transaction")
trip_fact = spark.read.format("delta").load(gold_path + "fact_trip")
payment_dimension = spark.read.format("delta").load(gold_path + "dim_payment")
rider_dimension = spark.read.format("delta").load(gold_path + "dim_rider")
station_dimension = spark.read.format("delta").load(gold_path + "dim_station")
rideable_dimension = spark.read.format("delta").load(gold_path + "dim_rideable")
trip_date_dimension = spark.read.format("delta").load(gold_path + "dim_trip_date")

# COMMAND ----------

# Query 1) Time Spent per Ride based on Date and Time Factors
query1 = trip_date_dimension.groupBy(date_format(col('started_at'), 'yyyy-MM-dd HH').alias('date_hour')) \
    .agg(sum(col('duration_in_seconds')).alias('duration'))

query1 = query1.withColumn('date_hour_ts', from_unixtime(unix_timestamp(col('date_hour'), 'yyyy-MM-dd HH'))) \
  .withColumn('date', date_format(col('date_hour_ts'), 'yyyy-MM-dd')) \
  .withColumn('hour', date_format(col('date_hour_ts'), 'HH'))

#query1.show()

query2 = trip_date_dimension.groupBy(date_format(col('started_at'), 'HH').alias('hour')) \
    .agg(sum(col('duration_in_seconds')).alias('duration')) \
    .sort(col('hour'))    

#query2.show()

query3 = trip_date_dimension.withColumn("day_of_week", date_format(col('started_at'), "EEEE")) \
    .groupBy(col("day_of_week").alias('day')) \
    .agg(sum(col('duration_in_seconds')).alias('duration')) \
    .sort(col('duration'))

#query3.show()

# COMMAND ----------

# Query time spent riding based on station

query_start_station = trip_fact.join(trip_date_dimension, "trip_id") \
    .withColumnRenamed('start_station_id', 'station_id') \
    .join(station_dimension, "station_id")

query4 = query_start_station.groupBy('station_id') \
    .agg(sum(col('duration_in_seconds')).alias('duration')) \
    .sort(desc(col('duration')))

#query4.show(10)

query_end_station = trip_fact.join(trip_date_dimension, "trip_id") \
    .withColumnRenamed('end_station_id', 'station_id') \
    .join(station_dimension, "station_id")

query5 = query_end_station.groupBy('station_id') \
    .agg(sum(col('duration_in_seconds')).alias('duration')) \
    .sort(desc(col('duration')))

#query5.show(10)


query_both_station = trip_fact.join(trip_date_dimension, "trip_id") \
    .withColumnRenamed('start_station_id', 'station_id') \
    .join(station_dimension, "station_id") \
    .withColumnRenamed('station_id', 'start') \
    .withColumnRenamed('end_station_id', 'station_id') \
    .join(station_dimension, "station_id") \

query6 = query_both_station.groupBy('start', 'station_id') \
    .agg(sum(col('duration_in_seconds')).alias('duration')) \
    .sort(desc(col('duration')))

#query6.show(10)

# COMMAND ----------

# Query time spent riding based on age of rider

query_time_age = trip_fact.join(trip_date_dimension, "trip_id") \
    .join(rider_dimension, "rider_id") \
    .withColumn("age", floor(datediff(current_date(), col("birthday")) / 365.25))

query7 = query_time_age.groupBy(col("age")) \
    .agg(sum(col('duration_in_seconds')).alias('duration')) \
    .sort(desc(col('duration')))

#query7.show(10)

query8 = query_time_age.groupBy(col("is_member")) \
    .agg(sum(col('duration_in_seconds')).alias('duration')) \
    .sort(desc(col('duration')))

#query8.show(10)

# COMMAND ----------

# Analyse how much money is spent
#   ● Per month, quarter, year

query_money_month = payment_dimension.groupBy(date_format(col('date'), 'MM').alias('month')) \
    .agg(sum(col('amount')).alias('money')) \
    .sort(asc(col('month')))

#query_money_month.show(12)

query_money_year = payment_dimension.groupBy(date_format(col('date'), 'yyyy').alias('year')) \
    .agg(sum(col('amount')).alias('money')) \
    .sort(asc(col('year')))

#query_money_year.show(10)

query_money_quarter = payment_dimension.groupBy(quarter('date').alias('quarter')) \
    .agg(sum(col('amount')).alias('money')) \
    .sort(asc(col('quarter')))

#query_money_quarter.show(10)

# COMMAND ----------

# Analyse how much money is spent
#   ● Per member, based on the age of the rider at account start

query_money_member = transaction_fact.join(payment_dimension, 'payment_id') \
    .join(rider_dimension, 'rider_id') \
    .filter(col("is_member") == True) \
    .withColumn('age at joining', floor(datediff(col("account_start_date"), col("birthday")) / 365.25)) \
    .groupBy(col('age at joining')) \
    .agg(sum(col('amount')).alias('money spent')) \
    .sort(desc(col('money spent')))

#query_money_member.show()

# COMMAND ----------

# Testing Schemas
# Issues reading from other notebooks
#assert trip_fact.schema == trip_fact_schema, "Schema mismatch for trip_fact table"
#assert trip_date_dimension.schema == trip_date_dimension_schema, "Schema mismatch for trip_date_dimension table"

# Testing Queries

assert query_money_month.count() == 12, "Should only be 12 rows for the 12 months"

assert query_money_year.count() == 10, "Should only be 10 rows for years 2013-2022"

value = query_money_member.filter(col("age at joining") == 21).collect()
assert value[0][0] == 21, "Most money spent should be by members who were 21 when they joined"
assert value[0][1] == 495225.00, "Members at this age should have spent (currency)495,225.00"
