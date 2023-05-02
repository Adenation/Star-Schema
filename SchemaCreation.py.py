# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession.builder.appName("CreateDatabase").getOrCreate()
spark.version

# COMMAND ----------

db_name = "aden_bronze_database"


# COMMAND ----------

bronze_db = spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(db_name))


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
    table_data.show(10)


# COMMAND ----------

# Silver Conversion

schema_list = [
    StructType([StructField("payment_id", IntegerType(),True),
    StructField("date", DateType(), False),
    StructField("amount", DecimalType(10, 2), False),
    StructField("rider_id", IntegerType(), False)])
    ,
    StructType([StructField("rider_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("address", StringType(), False),
    StructField("birthday", DateType(), False),
    StructField("account_start_date", DateType(), True),
    StructField("account_end_date", DateType(), True),
    StructField("is_member", BooleanType(), True)])
    ,
    StructType([StructField("trip_id", StringType(), False),
    StructField("rideable_type", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("rider_id", IntegerType(), True)])
    ,
    StructType([StructField("station_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("latitude", FloatType(), False),
    StructField("longitude", FloatType(), False)])
]
# In the specification start_station_id and end_station_id are listed as int types however they are foreign keys referring to a varchar field in station_id which causes type conflicts. As such I have changed them to string type.

# COMMAND ----------

print(schema_list[1][0].name)

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

payment_df.show(10)

# COMMAND ----------

rider_df.show(10)

# COMMAND ----------

trip_df.show(10)

# COMMAND ----------

station_df.show(10)

# COMMAND ----------

# Create Dimension Tables

# Create Rideable_Type Dimension 
rideable_dimension = trip_df.select("rideable_type").distinct() \
    .withColumn("rideable_type_id", monotonically_increasing_id())

rideable_dimension.show(10)
# Create Payments Dimension
payments_dimension = payment_df.select("payment_id", "date", "amount")

payments_dimension.show(10)
# Rider Dimension is the same as riders table

# Create Trip_Date Dimension
trip_date_dimension = trip_df.select("trip_id", "started_at", "ended_at").withColumn('duration in seconds', unix_timestamp(col('ended_at'))- unix_timestamp(col('started_at')))

trip_date_dimension.show(10)
# Station Dimension is the same as station table

# COMMAND ----------

# Create Fact Tables

# Join Trip Dimension to new Rideable Dimension
trip_df_2 = trip_df.join(rideable_dimension, "rideable_type")
# Create Trip Fact Table
trip_fact = trip_df_2.select("trip_id", "rider_id", "rideable_type_id", "start_station_id", "end_station_id")

trip_fact.show(10)
# Create Transaction Fact Table
transaction_fact = payment_df.select("payment_id", "rider_id")

transaction_fact.show(10)

# COMMAND ----------

# Write all of the Dataframes to Gold Tables

file_path = "aden_gold_"
transaction_fact.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(file_path + "fact_transaction")
trip_fact.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(file_path + "fact_trip")
payments_dimension.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(file_path + "dim_payment")
rider_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(file_path + "dim_rider")
station_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(file_path + "dim_station")
rideable_dimension.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(file_path + "dim_rideable")
trip_date_dimension.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(file_path + "dim_trip_time")

# COMMAND ----------

# Query 1) Time Spent per Ride based on Date and Time Factors
query1 = trip_date_dimension.groupBy(date_format(col('started_at'), 'yyyy-MM-dd HH').alias('date_hour')) \
    .agg(sum(col('duration in seconds')).alias('duration'))

query1 = query1.withColumn('date_hour_ts', from_unixtime(unix_timestamp(col('date_hour'), 'yyyy-MM-dd HH'))) \
  .withColumn('date', date_format(col('date_hour_ts'), 'yyyy-MM-dd')) \
  .withColumn('hour', date_format(col('date_hour_ts'), 'HH'))

query2 = trip_date_dimension.groupBy(date_format(col('started_at'), 'HH').alias('hour')) \
    .agg(sum(col('duration in seconds')).alias('duration')) \
    .sort(col('hour'))    

query2.show()

query3 = trip_date_dimension.withColumn("day_of_week", date_format(col('started_at'), "EEEE")) \
    .groupBy(col("day_of_week").alias('day')) \
    .agg(sum(col('duration in seconds')).alias('duration')) \
    .sort(col('duration'))

query3.show()

# COMMAND ----------

# Query time spent riding based on station

query_start_station = trip_fact.join(trip_date_dimension, "trip_id") \
    .withColumnRenamed('start_station_id', 'station_id') \
    .join(station_df, "station_id")

query4 = query_start_station.groupBy('station_id') \
    .agg(sum(col('duration in seconds')).alias('duration')) \
    .sort(desc(col('duration')))

query4.show(10)

query_end_station = trip_fact.join(trip_date_dimension, "trip_id") \
    .withColumnRenamed('end_station_id', 'station_id') \
    .join(station_df, "station_id")

query5 = query_end_station.groupBy('station_id') \
    .agg(sum(col('duration in seconds')).alias('duration')) \
    .sort(desc(col('duration')))

query5.show(10)


query_both_station = trip_fact.join(trip_date_dimension, "trip_id") \
    .withColumnRenamed('start_station_id', 'station_id') \
    .join(station_df, "station_id") \
    .withColumnRenamed('station_id', 'start') \
    .withColumnRenamed('end_station_id', 'station_id') \
    .join(station_df, "station_id") \

query6 = query_both_station.groupBy('start', 'station_id') \
    .agg(sum(col('duration in seconds')).alias('duration')) \
    .sort(desc(col('duration')))

query6.show(10)

# COMMAND ----------

# Query time spent riding based on age of rider

query_time_age = trip_fact.join(trip_date_dimension, "trip_id") \
    .join(rider_df, "rider_id") \
    .withColumn("age", floor(datediff(current_date(), col("birthday")) / 365.25))

query7 = query_time_age.groupBy(col("age")) \
    .agg(sum(col('duration in seconds')).alias('duration')) \
    .sort(desc(col('duration')))

query7.show(10)

query8 = query_time_age.groupBy(col("is_member")) \
    .agg(sum(col('duration in seconds')).alias('duration')) \
    .sort(desc(col('duration')))

query8.show(10)

# COMMAND ----------

# Analyse how much money is spent
#   ● Per month, quarter, year

query_money_month = payments_dimension.groupBy(date_format(col('date'), 'MM').alias('month')) \
    .agg(sum(col('amount')).alias('money')) \
    .sort(asc(col('month')))

query_money_month.show(12)

query_money_year = payments_dimension.groupBy(date_format(col('date'), 'yyyy').alias('year')) \
    .agg(sum(col('amount')).alias('money')) \
    .sort(asc(col('year')))

query_money_year.show(10)

query_money_quarter = payments_dimension.groupBy(quarter('date').alias('quarter')) \
    .agg(sum(col('amount')).alias('money')) \
    .sort(asc(col('quarter')))

query_money_quarter.show(10)

# COMMAND ----------

# Analyse how much money is spent
#   ● Per member, based on the age of the rider at account start

query_money_member = transaction_fact.join(payments_dimension, 'payment_id') \
    .join(rider_df, 'rider_id') \
    .filter(col("is_member") == True) \
    .withColumn('age at joining', floor(datediff(col("account_start_date"), col("birthday")) / 365.25)) \
    .groupBy(col('age at joining')) \
    .agg(sum(col('amount')).alias('money spent')) \
    .sort(desc(col('money spent')))
query_money_member.show(10)

# COMMAND ----------

# Query
# EXTRA CREDIT - Analyze how much money is spent per member
#   ● Based on how many rides the rider averages per month
#   ● Based on how many minutes the rider spends on a bike per month

#read query plz
query_total_spend_per_member = transaction_fact.join(payments_dimension, 'payment_id') \
    .join(rider_df, 'rider_id') \
    .filter(col("is_member") == True) \
    .groupBy("rider_id") \
    .agg(sum(col('amount')))

query_rides_average_per_month = trip_fact.join(rider_df, 'rider_id') \
    .filter(col("is_member") == True) \
    .groupBy("rider_id").count() \
    .withColumn('months since joining', floor(datediff(current_date(), col("account_start_date")) / 12))

query_money_member.show(10)
query_rides_average_per_month.show(10)

# COMMAND ----------

tf = trip_fact.groupBy(col("rider_id")).count().sort(desc(col("count")))

tf.show(100)

# COMMAND ----------


