# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession.builder.appName("CreateDatabase").getOrCreate()


# COMMAND ----------

db_name = "aden_bronze_database"


# COMMAND ----------

bronze_db = spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(db_name))


# COMMAND ----------

bronze_db.show(10)

# COMMAND ----------

spark.sql("USE {}".format(db_name))


# COMMAND ----------

table_list = [["payments",4, 0], ["riders",8, 1], ["trips",7, 2], ["stations",4, 3]]


# COMMAND ----------

# Avoid data loss by ensuring all columns are imported as strings
for table_name in table_list:
    file_name = "/FileStore/tables/{}.csv".format(table_name[0])
    # Define the schema with all columns set to StringType()
    all_string = StructType([StructField(str(i), StringType(), True) for i in range(0, table_name[1])])
    table_data = spark.read.format("csv") \
        .option("header", False) \
        .schema(all_string) \
        .load(file_name)

    # Create the table
    table_data.write.format("parquet").mode("overwrite").saveAsTable("aden_bronze_" + table_name[0])


# COMMAND ----------

# Silver Conversion

schema_list = [
    [
        StructType([StructField("payment_id", IntegerType(), False)]),
        StructType([StructField("date", DateType(), False)]),
        StructType([StructField("amount", DecimalType(10, 2), False)]),
        StructType([StructField("rider_id", IntegerType(), False)])
    ],
    [
        StructType([StructField("rider_id", IntegerType(), False)]),
        StructType([StructField("first_name", StringType(), False)]),
        StructType([StructField("last_name", StringType(), False)]),
        StructType([StructField("address", StringType(), False)]),
        StructType([StructField("birthday", DateType(), False)]),
        StructType([StructField("account_start_date", DateType(), True)]),
        StructType([StructField("account_end_date", DateType(), True)]),
        StructType([StructField("is_member", BooleanType(), True)])
    ],
    [
        StructType([StructField("trip_id", StringType(), False)]),
        StructType([StructField("rideable_type", StringType(), False)]),
        StructType([StructField("started_at", TimestampType(), False)]),
        StructType([StructField("ended_at", TimestampType(), True)]),
        StructType([StructField("start_station_id", IntegerType(), True)]),
        StructType([StructField("end_station_id", IntegerType(), True)]),
        StructType([StructField("rider_id", IntegerType(), True)])
    ],
    [
        StructType([StructField("station_id", StringType(), False)]),
        StructType([StructField("name", StringType(), False)]),
        StructType([StructField("latitude", FloatType(), False)]),
        StructType([StructField("longitude", FloatType(), False)])
    ]
]


# COMMAND ----------


for i in range(4):
    file_name = "/user/hive/warehouse/aden_bronze_database.db/aden_bronze_{}".format(table_list[i][0])
    table_data = spark.read.format("parquet") \
        .option("header", False) \
        .schema(schema_list[i][0]) \
        .load(file_name)
    
    # Create the table
    table_data.write.format("delta").mode("overwrite").option("mergeSchema", True).saveAsTable("aden_silver_" + table_list[i][0])


# COMMAND ----------

gold_tables = []
# Loop through each table and convert it into star schema
for table_name in table_list:
    # Establish File Name
    file_name = "/user/hive/warehouse/aden_bronze_database.db/aden_silver_{}".format(table_name[0])
    # Read the Delta table
    table_data = spark.read.format("delta").load(file_name)

    # Add column names to the table
    for i in range(len(schema_list[table_name[2]])):
        table_data = table_data.withColumnRenamed(f"{i}", schema_list[table_name[2]][i][0].name) 

    gold_tables.append(table_data)

# COMMAND ----------

gold_tables[2].show(1000)

# COMMAND ----------

payment_fact = gold_tables[0].join(gold_tables[1], "rider_id")
payment_fact.show(10)

# COMMAND ----------


trip_fact = gold_tables[2].join(gold_tables[1], "rider_id").join(gold_tables[3], "start_station_id").join(station_dim, "end_station_id")

for table in gold_tables:
    table.write.format("delta").mode("overwrite").saveAsTable("aden_gold_" + table_list[i][0])

# COMMAND ----------

file_path = "/user/hive/warehouse/aden_bronze_database.db/aden_silver_"
# 
payments_file_path = file_path + table_list[0][0]
payment_df = spark.read.format("delta").option("header", True).load(payments_file_path)
#
riders_file_path = file_path + table_list[1][0]
rider_df = spark.read.format("delta").option("header", True).load(riders_file_path)
#
trips_file_path = file_path + table_list[2][0]
trip_df = spark.read.format("delta").option("header", True).load(trips_file_path)
#
stations_file_path = file_path + table_list[3][0]
station_df = spark.read.format("delta").option("header", True).load(stations_file_path)


# COMMAND ----------

# Create Fact Table 
trip_fact_df = payment_df.join(rider_df, "rider_id")

trip_fact.show(10)

# COMMAND ----------



# Create dimension tables
rider_dim = rider_df.select(col("rider_id"), col("first_name"), col("last_name"), col("address"), col("birthday"), col("account_start_date"), col("account_end_date"), col("is_member"))
station_dim = station_df.select(col("station_id"), col("name"), col("latitude"), col("longitude"))

# Create fact tables
payment_fact = payment_df.join(rider_dim, "rider_id")
trip_fact = trip_df.join(rider_dim, "rider_id").join(station_dim, "start_station_id").join(station_dim, "end_station_id")

# Write dimension tables to Delta
rider_dim.write.format("delta").mode("overwrite").save("/path/to/rider_dim")
station_dim.write.format("delta").mode("overwrite").save("/path/to/station_dim")

# Write fact tables to Delta
payment_fact.write.format("delta").mode("overwrite").save("/path/to/payment_fact")
trip_fact.write.format("delta").mode("overwrite").save("/path/to/trip_fact")

# COMMAND ----------

# Add column names to tale
