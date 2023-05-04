# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

folder_path = "/tmp/aden/"
silver_path = folder_path + "silver/"
gold_path = folder_path + "gold/"

# COMMAND ----------

# Read Silver Tables
payment_df = spark.read.format("delta").option("header", True).load(silver_path + "payments")
rider_df = spark.read.format("delta").option("header", True).load(silver_path + "riders")
trip_df = spark.read.format("delta").option("header", True).load(silver_path + "trips")
station_df = spark.read.format("delta").option("header", True).load(silver_path + "stations")

# COMMAND ----------

# Create Dimension Tables

# Create Rideable_Type Dimension 
rideable_dimension = trip_df.select("rideable_type").distinct() \
    .withColumn("rideable_type_id", monotonically_increasing_id())

#rideable_dimension.show(10)
# Create Payments Dimension
payments_dimension = payment_df.select("payment_id", "date", "amount")

#payments_dimension.show(10)
# Rider Dimension is the same as riders table

# Create Trip_Date Dimension
trip_date_dimension = trip_df.select("trip_id", "started_at", "ended_at").withColumn('duration_in_seconds', unix_timestamp(col('ended_at'))- unix_timestamp(col('started_at')))

#trip_date_dimension.show(10)
# Station Dimension is the same as station table

# COMMAND ----------

# Create Fact Tables

# Join Trip Dimension to new Rideable Dimension
trip_df_2 = trip_df.join(rideable_dimension, "rideable_type")
# Create Trip Fact Table
trip_fact = trip_df_2.select("trip_id", "rider_id", "rideable_type_id", "start_station_id", "end_station_id")

#trip_fact.show(10)
# Create Transaction Fact Table
transaction_fact = payment_df.select("payment_id", "rider_id")

#transaction_fact.show(10)

# COMMAND ----------

# Write all of the Dataframes to Gold Tables
transaction_fact.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(gold_path + "fact_transaction")
trip_fact.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(gold_path + "fact_trip")
payments_dimension.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(gold_path + "dim_payment")
rider_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(gold_path + "dim_rider")
station_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(gold_path + "dim_station")
rideable_dimension.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(gold_path + "dim_rideable")
trip_date_dimension.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(gold_path + "dim_trip_date")

# COMMAND ----------


