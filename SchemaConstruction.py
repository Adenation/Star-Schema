# Databricks notebook source
# Imports

from pyspark.sql.types import *

# COMMAND ----------

# Directories
folder_path = "/tmp/aden/"
bronze_path = folder_path + "bronze/"
silver_path = folder_path + "silver/"
gold_path = folder_path + "gold/"

dbutils.fs.mkdirs(folder_path)

# COMMAND ----------

# Get Schemas from Schema Creation Notebook
dbutils.notebook.run("/Repos/aden.victor@qualyfi.co.uk/Star-Schema/SchemaCreation", timeout_seconds=1800)
df = spark.sparkContext.emptyRDD()
db_name = "aden_star_schema"
db = spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(db_name))
spark.sql("USE {}".format(db_name))

# Create Empty Dataframes

payments_bronze_df = spark.createDataFrame(df, payment_bronze_schema)
riders_bronze_df = spark.createDataFrame(df, rider_bronze_schema)
stations_bronze_df = spark.createDataFrame(df, station_bronze_schema)
trips_bronze_df = spark.createDataFrame(df, trip_bronze_schema)

payments_silver_df = spark.createDataFrame(df, payment_silver_schema)
riders_silver_df = spark.createDataFrame(df, rider_silver_schema)
stations_silver_df = spark.createDataFrame(df, station_silver_schema)
trips_silver_df = spark.createDataFrame(df, trip_silver_schema)

transaction_fact_df = spark.createDataFrame(df, transaction_fact_schema)
trip_fact_df = spark.createDataFrame(df, trip_fact_schema)
payment_dimension_df = spark.createDataFrame(df, payment_dimension_schema)
rider_dimension_df = spark.createDataFrame(df, rider_dimension_schema)
station_dimension_df = spark.createDataFrame(df, station_dimension_schema)
rideable_dimension_df = spark.createDataFrame(df, rideable_dimension_schema)
trip_date_dimension_df = spark.createDataFrame(df, trip_date_dimension_schema)

# Write empty tables to be populated later
payments_bronze_df.write.format("parquet").mode("overwrite").save(bronze_path + "payments")
riders_bronze_df.write.format("parquet").mode("overwrite").save(bronze_path + "riders")
stations_bronze_df.write.format("parquet").mode("overwrite").save(bronze_path + "stations")
trips_bronze_df.write.format("parquet").mode("overwrite").save(bronze_path + "trips")

payments_silver_df.write.format("delta").mode("overwrite").save(silver_path + "payments")
riders_silver_df.write.format("delta").mode("overwrite").save(silver_path + "riders")
stations_silver_df.write.format("delta").mode("overwrite").save(silver_path + "stations")
trips_silver_df.write.format("delta").mode("overwrite").save(silver_path + "trips")

transaction_fact_df.write.format("delta").mode("overwrite").save(gold_path + "fact_transactions")
trip_fact_df.write.format("delta").mode("overwrite").save(gold_path + "fact_trips")
payment_dimension_df.write.format("delta").mode("overwrite").save(gold_path + "dim_payments")
rider_dimension_df.write.format("delta").mode("overwrite").save(gold_path + "dim_riders")
station_dimension_df.write.format("delta").mode("overwrite").save(gold_path + "dim_stations")
rideable_dimension_df.write.format("delta").mode("overwrite").save(gold_path + "dim_rideables")
trip_date_dimension_df.write.format("delta").mode("overwrite").save(gold_path + "dim_trip_dates")
