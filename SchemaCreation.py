# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

# Bronze Schema

# Schema dynamically set based on number of columns in table to all StringType()

payment_bronze_schema = StructType([StructField(str(i), StringType(), True) for i in range(0,  4)])

rider_bronze_schema = StructType([StructField(str(i), StringType(), True) for i in range(0,  8)])

station_bronze_schema = payment_bronze_schema

trip_bronze_schema = StructType([StructField(str(i), StringType(), True) for i in range(0,  7)])

# COMMAND ----------

# Silver Schema

payment_silver_schema = StructType([
    StructField("payment_id", IntegerType(),True),
    StructField("date", DateType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("rider_id", IntegerType(), True)
    ])
rider_silver_schema = StructType([
    StructField("rider_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("birthday", DateType(), True),
    StructField("account_start_date", DateType(), True),
    StructField("account_end_date", DateType(), True),
    StructField("is_member", BooleanType(), True)
    ])
trip_silver_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("rider_id", IntegerType(), True)
    ])
station_silver_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)
    ])

silver_schema_list = [payment_silver_schema, rider_silver_schema, station_silver_schema, trip_silver_schema]
# In the specification start_station_id and end_station_id are listed as int types however they are foreign keys referring to a varchar field in station_id which causes type conflicts. As such I have changed them to string type.

# COMMAND ----------

# Gold Schema

transaction_fact_schema = StructType([
    StructField("payment_id", IntegerType(),True),
    StructField("rider_id", IntegerType(), True),
])
trip_fact_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("rider_id", IntegerType(), True),
    StructField("rideable_type_id", LongType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True)
])
payment_dimension_schema = StructType([
    StructField("payment_id", IntegerType(),True),
    StructField("date", DateType(), True),
    StructField("amount", DecimalType(10, 2), True)
    ])
rider_dimension_schema = rider_silver_schema
station_dimension_schema = station_silver_schema
rideable_dimension_schema = StructType([
    StructField("rideable_type_id", LongType(), True),
    StructField("rideable_type", StringType(), True)
])
trip_date_dimension_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("duration_in_seconds", LongType(), True),
])

gold_schema_list = [transaction_fact_schema, trip_fact_schema, payment_dimension_schema, rider_dimension_schema, station_dimension_schema, rideable_dimension_schema, trip_date_dimension_schema]
