# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

folder_path = "/tmp/aden/"
bronze_path = folder_path + "bronze/"
silver_path = folder_path + "silver/"
bronze_tables = ["payments", "riders", "stations", "trips"]

# COMMAND ----------

# Read silver tables to acquire schemas
payments_silver_df = spark.read.format("delta") \
    .option("header", False) \
    .load(silver_path + "payments")
payment_silver_schema = payments_silver_df.schema
riders_silver_df = spark.read.format("delta") \
    .option("header", False) \
    .load(silver_path + "riders")
rider_silver_schema = riders_silver_df.schema    
stations_silver_df = spark.read.format("delta") \
    .option("header", False) \
    .load(silver_path + "stations")
station_silver_schema = stations_silver_df.schema    
trips_silver_df = spark.read.format("delta") \
    .option("header", False) \
    .load(silver_path + "trips")
trip_silver_schema = trips_silver_df.schema
silver_schema_list = [payment_silver_schema, rider_silver_schema, station_silver_schema, trip_silver_schema]
# In the specification start_station_id and end_station_id are listed as int types however they are foreign keys referring to a varchar field in station_id which causes type conflicts. As such I have changed them to string type.

# COMMAND ----------

# Read Bronze Tables & Write Silver Tables
for j in range(4):
    file_name = bronze_path + "{}".format(bronze_tables[j])
    table_data = spark.read.format("delta") \
        .option("header", False) \
        .load(file_name)
    #table_data.show(3)
    #print("Before Write and After Write") 
    ## Reading in data with schema created null values in all cells
    # Apply the new schema and cast the values
    for i, c in enumerate(table_data.columns):
        new_type = silver_schema_list[j][i]
        #print(new_type.dataType)
        if isinstance(new_type.dataType, TimestampType):
            table_data = table_data.withColumn(c, to_timestamp(table_data[c], "dd/MM/yyyy HH:mm"))
            # If timestamp is not in this format will need to update code to handle it
        else:
            table_data = table_data.withColumn(c, table_data[c].cast(new_type.dataType))

        table_data = table_data.withColumnRenamed(table_data.columns[i], new_type.name)
    
    
    # Create the table
    table_data.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(silver_path + bronze_tables[j])
    #table_data.show(3)
