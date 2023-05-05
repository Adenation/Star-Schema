# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

#Directories
folder_path = "/tmp/aden/"
data_path = folder_path + "landing/"

# COMMAND ----------

# Data Ingestion
!wget "https://raw.githubusercontent.com/Adenation/Star-Schema/main/Star%20Schema/payments.csv" -P /dbfs/tmp/aden/landing/
!wget "https://raw.githubusercontent.com/Adenation/Star-Schema/main/Star%20Schema/riders.csv" -P /dbfs/tmp/aden/landing/
!wget "https://raw.githubusercontent.com/Adenation/Star-Schema/main/Star%20Schema/stations.csv" -P /dbfs/tmp/aden/landing/
!wget "https://raw.githubusercontent.com/Adenation/Star-Schema/main/Star%20Schema/trips.csv" -P /dbfs/tmp/aden/landing/

# COMMAND ----------


# Set the path to the directory

bronze_path = folder_path + "bronze/"

# Read bronze tables to acquire schemas
payments_bronze_df = spark.read.format("delta") \
    .option("header", False) \
    .load(bronze_path + "payments")
payment_bronze_schema = payments_bronze_df.schema
riders_bronze_df = spark.read.format("delta") \
    .option("header", False) \
    .load(bronze_path + "riders")
rider_bronze_schema = riders_bronze_df.schema    
stations_bronze_df = spark.read.format("delta") \
    .option("header", False) \
    .load(bronze_path + "stations")
station_bronze_schema = stations_bronze_df.schema    
trips_bronze_df = spark.read.format("delta") \
    .option("header", False) \
    .load(bronze_path + "trips")
trip_bronze_schema = trips_bronze_df.schema

# read the CSV file from GitHub as a PySpark DataFrame
payments_df = spark.read.format("csv") \
    .option("header", False) \
    .schema(payment_bronze_schema) \
    .load(data_path + "payments.csv")

riders_df = spark.read.format("csv") \
    .option("header", False) \
    .schema(rider_bronze_schema) \
    .load(data_path + "riders.csv")

stations_df = spark.read.format("csv") \
    .option("header", False) \
    .schema(station_bronze_schema) \
    .load(data_path + "stations.csv")

trips_df = spark.read.format("csv") \
    .option("header", False) \
    .schema(trip_bronze_schema) \
    .load(data_path + "trips.csv")

# Write to Bronze Tables
payments_df.write.format("delta").mode("overwrite").save(bronze_path + "payments")
riders_df.write.format("delta").mode("overwrite").save(bronze_path + "riders")
stations_df.write.format("delta").mode("overwrite").save(bronze_path + "stations")
trips_df.write.format("delta").mode("overwrite").save(bronze_path + "trips")


