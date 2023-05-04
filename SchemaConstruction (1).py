# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession.builder.appName("CreateDatabase").getOrCreate()
spark.version

# COMMAND ----------

df = spark.sparkContext.emptyRDD()

# COMMAND ----------

db_name = "aden_bronze_database"


# COMMAND ----------

bronze_db = spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(db_name))


# COMMAND ----------

spark.sql("USE {}".format(db_name))


# COMMAND ----------

table_list = [["payments",4, 0], ["riders",8, 1], ["trips",7, 2], ["stations",4, 3]]


# COMMAND ----------

file_name = "/user/hive/warehouse/aden_bronze_database.db/aden_bronze_trips"
table_data = spark.read.format("parquet") \
    .option("header", False) \
    .load(file_name)
#table_data.show(3)
df = table_data


## Reading in data with schema created null values in all cells
# Apply the new schema and cast the values
for i, c in enumerate(df.columns):
    df = df.withColumn(c, df[c].cast(schema_list[2][i].dataType)) \
        .withColumnRenamed(df.columns[i], schema_list[2][i].name)

df.show(10)

# COMMAND ----------



# COMMAND ----------

for j in range(4):
    file_name = "/user/hive/warehouse/aden_bronze_database.db/aden_bronze_{}".format(table_list[j][0])
    table_data = spark.read.format("parquet") \
        .option("header", False) \
        .load(file_name)
    table_data.show(3)
    print("Before Write and After Write") 
    ## Reading in data with schema created null values in all cells
    # Apply the new schema and cast the values
    for i, c in enumerate(table_data.columns):
        new_type = schema_list[j][i]
        print(new_type.dataType)
        if isinstance(new_type.dataType, TimestampType):
            table_data = table_data.withColumn(c, to_timestamp(table_data[c], "dd/MM/yyyy HH:mm"))
            # If timestamp is not in this format will need to update code to handle it
        else:
            table_data = table_data.withColumn(c, table_data[c].cast(new_type.dataType))

        table_data = table_data.withColumnRenamed(table_data.columns[i], new_type.name)
    
    
    # Create the table
    table_data.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("aden_silver_" + table_list[j][0])
    table_data.show(3)


# COMMAND ----------

file_path = "/user/hive/warehouse/aden_bronze_database.db/aden_silver_"



# COMMAND ----------

payment_df.show(10)

# COMMAND ----------

rider_df.show(10)

# COMMAND ----------

trip_df.show(10)

# COMMAND ----------

station_df.show(10)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


