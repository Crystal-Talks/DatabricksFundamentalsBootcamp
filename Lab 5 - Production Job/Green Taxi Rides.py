# Databricks notebook source
# MAGIC %md
# MAGIC ###Define Root Path and Catalog
# MAGIC As you have copied earlier in Lab 2.
# MAGIC
# MAGIC 1. If Unity Catalog is not enabled:
# MAGIC     - Use file path: "/FileStore/"
# MAGIC     - Use catalog: hive_metastore
# MAGIC
# MAGIC 2. If Unity Catalog is enabled, use Volume:
# MAGIC     - Use file path - "/Volumes/oreillycatalog/default/datafiles/"
# MAGIC     - Use catalog: oreillycatalog

# COMMAND ----------

#Uncomment line whichever is applicable and execute

# If Unity Catalog is not enabled
#sourceFolderPath = "/FileStore/"

# If Unity Catalog is enabled
sourceFolderPath = "/Volumes/oreillycatalog/default/datafiles/"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Uncomment line whichever is applicable and execute
# MAGIC
# MAGIC -- If Unity Catalog is not enabled
# MAGIC -- USE CATALOG hive_metastore
# MAGIC
# MAGIC -- If Unity Catalog is enabled
# MAGIC USE CATALOG oreillycatalog

# COMMAND ----------

# MAGIC %md ### A. Define File Path

# COMMAND ----------

# Define input & output paths

greenTaxisInputPath = sourceFolderPath + "Raw/GreenTaxis_202501.csv"

greenTaxisOutputPath = sourceFolderPath + "Output/GreenTaxis"

# COMMAND ----------

# MAGIC %md ### B. Read File by Applying Schema

# COMMAND ----------

# Create schema for Green Taxi Data

from pyspark.sql.functions import *
from pyspark.sql.types import *
  
greenTaxiSchema = (
            StructType()               
               .add("VendorId", "integer")
               .add("lpep_pickup_datetime", "timestamp")
               .add("lpep_dropoff_datetime", "timestamp")
               .add("store_and_fwd_flag", "string")
               .add("RatecodeID", "integer")
               .add("PULocationID", "integer")
               .add("DOLocationID", "integer")
  
              .add("passenger_count", "integer")
              .add("trip_distance", "double")
              .add("fare_amount", "double")
              .add("extra", "double")
              .add("mta_tax", "double")
              .add("tip_amount", "double")
  
              .add("tolls_amount", "double")
              .add("ehail_fee", "double")
              .add("improvement_surcharge", "double")
              .add("total_amount", "double")
              .add("payment_type", "integer")
              .add("trip_type", "integer")
         )

# COMMAND ----------

# Create DataFrame by applying the schema

greenTaxiDF = (
                    spark
                        .read
                        .option("header", "true")

                        .schema(greenTaxiSchema)

                        .csv(greenTaxisInputPath)
                )

# Print schema
greenTaxiDF.printSchema()

# COMMAND ----------

# MAGIC %md ### C. Clean Data

# COMMAND ----------

greenTaxiDF = (
                  greenTaxiDF
    
                      .where("passenger_count > 0")

                      .filter(col("trip_distance") > 0.0)
               )

# COMMAND ----------

greenTaxiDF = (
                   greenTaxiDF    
                          .na.drop('all')
               )

# COMMAND ----------

defaultValueMap = {'payment_type': 5, 'RateCodeID': 1}


greenTaxiDF = (
                   greenTaxiDF    
                      .na.fill(defaultValueMap)
               )

# COMMAND ----------

greenTaxiDF = (
                   greenTaxiDF
                          .dropDuplicates()
               )

# COMMAND ----------

greenTaxiDF = (
    
                    greenTaxiDF
                        .where("lpep_pickup_datetime >= '2025-01-01' AND lpep_dropoff_datetime < '2025-02-01'")
               )

# COMMAND ----------

# MAGIC %md ### D. Transform Data

# COMMAND ----------

greenTaxiDF = (
                   greenTaxiDF

                        # Select only limited columns
                        .select(
                                  "VendorId",
                             
                                  col("passenger_count").cast(IntegerType()),
                            
                                  column("trip_distance").alias("TripDistance"),
                            
                                  greenTaxiDF.lpep_pickup_datetime,
                            
                                  "lpep_dropoff_datetime",
                                  "PULocationID",
                                  "DOLocationID",
                                  "RatecodeID",
                                  "total_amount",
                                  "payment_type"
                               )
               )

# COMMAND ----------

greenTaxiDF = (
                   greenTaxiDF                        
                        
                        .withColumnRenamed("passenger_count", "PassengerCount")
    
                        .withColumnRenamed("lpep_pickup_datetime", "PickupTime")
                        .withColumnRenamed("lpep_dropoff_datetime", "DropTime")
                        .withColumnRenamed("PUlocationID", "PickupLocationId")
                        .withColumnRenamed("DOlocationID", "DropLocationId")
                        .withColumnRenamed("total_amount", "TotalAmount")
                        .withColumnRenamed("payment_type", "PaymentType")    
               )

# COMMAND ----------

# Create derived columns for year, month and day
greenTaxiDF = (
                  greenTaxiDF
    
                        .withColumn("TripYear", year(col("PickupTime")))
    
                        .select(
                                    "*",
                            
                                    expr("month(PickupTime) AS TripMonth"),
                            
                                    dayofmonth(col("PickupTime")).alias("TripDay")
                               )
               )

# COMMAND ----------

tripTimeInSecondsExpr = unix_timestamp(col("DropTime")) - unix_timestamp(col("PickupTime"))


tripTimeInMinutesExpr = round(tripTimeInSecondsExpr / 60)


greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripTimeInMinutes", tripTimeInMinutesExpr)
               )

# COMMAND ----------

tripTypeColumn = (
                    when(
                            col("RatecodeID") == 6,
                              "SharedTrip"
                         )
                    .otherwise("SoloTrip")
                 )    


greenTaxiDF = (
                  greenTaxiDF
    
                        .withColumn("TripType", tripTypeColumn)
               )

# COMMAND ----------

# MAGIC %md ### E. Save Data to Data Lake as Spark (Delta) Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS TaxiDB;

# COMMAND ----------

(
    greenTaxiDF
        .write

        .mode("overwrite")

        .format("delta")
        
        .saveAsTable("taxidb.greentaxis")
)