# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

folder_path = "/tmp/aden/"
bronze_path = folder_path + "bronze/"
silver_path = folder_path + "silver/"
bronze_tables = ["payments", "riders", "stations", "trips"]

# COMMAND ----------

# Get Schemas from Schema Creation Notebook
dbutils.notebook.run("/Repos/aden.victor@qualyfi.co.uk/Star-Schema/SchemaCreation", timeout_seconds=1800)
# Read Bronze Tables & Write Silver Tables
for j in range(4):
    file_name = bronze_path + "{}".format(bronze_tables[j])
    table_data = spark.read.format("parquet") \
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
